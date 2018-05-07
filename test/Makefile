default: standalone distributed

standalone:
	@dpe_HOST= dpe_WORKERS=2 tape "./**/*.js"

distributed:
	@../bin/server.js -l 2 >/tmp/dpe-server.log 2>&1 & pid=$$!; \
	dpe_HOST=localhost tape "./**/*.js"; \
	res=$$?; kill $$pid; exit $$res

%:
	dpe_HOST= dpe_WORKERS=2 tape "./**/*$@*.js"
