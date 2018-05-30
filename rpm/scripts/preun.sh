set -e
if [ "$1" = 0 ]; then
  /sbin/service mad stop > /dev/null 2>&1
  /sbin/chkconfig --del mad
fi
exit 0
