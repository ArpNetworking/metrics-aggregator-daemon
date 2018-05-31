set -e
/sbin/chkconfig --add mad
mkdir -p /opt/mad/logs
chown mad:mad /opt/mad/logs
mkdir -p /opt/mad/config/pipelines
mkdir -p /var/run/mad
chown mad:mad /var/run/mad
