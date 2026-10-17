"""The link to AWS, measured: TCP connect and one HTTPS request to the S3
endpoint in us-east-1, repeated. Printed next to every duration measured
on AWS (P9): the same job ran 34 s or 95 s a batch depending on the link,
and a measurement without its conditions cannot be compared.

Usage: python3 scripts/link_probe.py [samples]   (default 20, 1 s apart)
Standard library only; no credentials (an anonymous HEAD returns 403 or 404,
which still times the full round trip: DNS, TCP, TLS, HTTP).
"""

import socket
import statistics
import sys
import time
import urllib.error
import urllib.request

HOST = "s3.us-east-1.amazonaws.com"


def tcp_connect_ms() -> float:
    address = socket.getaddrinfo(HOST, 443, type=socket.SOCK_STREAM)[0][4]
    start = time.perf_counter()
    with socket.create_connection(address[:2], timeout=10):
        return (time.perf_counter() - start) * 1000


def https_request_ms() -> float:
    # A new connection each time, as a job's first request after idling:
    # DNS lookup, TCP, TLS handshake and the request itself.
    start = time.perf_counter()
    request = urllib.request.Request(f"https://{HOST}/", method="HEAD")
    try:
        urllib.request.urlopen(request, timeout=10).close()
    except urllib.error.HTTPError:
        pass  # 403/404 without credentials: the round trip is what counts
    return (time.perf_counter() - start) * 1000


def summary(name: str, values: list[float], failures: int) -> str:
    if not values:
        return f"{name}: all {failures} failed"
    q = statistics.quantiles(values, n=10) if len(values) > 1 else values * 9
    return (
        f"{name}: median {statistics.median(values):.0f} ms, "
        f"p90 {q[8]:.0f} ms, max {max(values):.0f} ms"
        + (f", {failures} failed" if failures else "")
    )


def main() -> None:
    samples = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    results = {"tcp connect": ([], 0), "https request": ([], 0)}
    for i in range(samples):
        for name, probe in (
            ("tcp connect", tcp_connect_ms),
            ("https request", https_request_ms),
        ):
            values, failures = results[name]
            try:
                values.append(probe())
            except OSError:
                results[name] = (values, failures + 1)
        if i < samples - 1:
            time.sleep(1)
    print(time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()), HOST)
    for name, (values, failures) in results.items():
        print(summary(name, values, failures))


if __name__ == "__main__":
    main()
