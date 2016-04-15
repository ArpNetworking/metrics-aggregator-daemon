getent group mad >/dev/null || groupadd -r mad
getent passwd mad >/dev/null || \
    useradd -r -g mad -d /opt/mad -s /sbin/nologin \
    -c "Account used for isolation of metrics aggregator daemon" mad
