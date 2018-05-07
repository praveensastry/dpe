#!/bin/sh
# Docker entrypoint (pid 1), run as root

[ "$1" = "sh" ] && exec "$@"

webserver() {
	mkdir -p /www/tmp
	ln -sf /tmp/dpe /www/tmp/dpe
	httpd -h /www
}

trap 'echo terminated; kill $pid' SIGTERM

case $1 in
(dpe-server|dpe-worker)
	webserver
	log=/var/log/$1.log
	[ -f $log ] && mv $log $log.old
	cmd="cd; ulimit -c unlimited; env; echo $@; exec $@"
	su -s /bin/sh -c "$cmd" dpe 2>&1 | tee /var/log/$1.log & pid=$!
	wait $pid
	;;
esac
