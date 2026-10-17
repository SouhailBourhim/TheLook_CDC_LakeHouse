# Grafana for the dashboards (P9b, ADR 023): the pinned image plus the
# Amazon Athena data source plugin, downloaded once at build time and
# checked against grafana.com's published SHA-256 (not fetched at each
# start: a start must not depend on grafana.com, and the bytes must be the
# ones checked). Data sources and dashboards come from provisioning/ and
# dashboards/: Grafana keeps no state worth a volume.
FROM grafana/grafana:13.2.3@sha256:b28bae15e219c998fb0e0424ed724930cc61b1f61fb404d47c862f9a23f9e572

USER root
# Plugin 3.3.6 (2026-09-28), Apache 2.0, needs Grafana >= 10.4.
ADD --checksum=sha256:e577dac907e2635347f4c56b3b77662591ac7e0c7aa565b1601cd7e85292a643 \
    "https://grafana.com/api/plugins/grafana-athena-datasource/versions/3.3.6/download?os=linux&arch=amd64" \
    /tmp/athena.zip
# Outside /var/lib/grafana, so a volume mounted there could never hide it.
RUN mkdir -p /opt/grafana-plugins \
 && unzip -q /tmp/athena.zip -d /opt/grafana-plugins \
 && rm /tmp/athena.zip \
 && chown -R grafana:root /opt/grafana-plugins
ENV GF_PATHS_PLUGINS=/opt/grafana-plugins

COPY --chown=grafana:root provisioning /etc/grafana/provisioning
COPY --chown=grafana:root dashboards /etc/grafana/dashboards
USER grafana
