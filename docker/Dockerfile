from alpine:edge

RUN apk add --no-cache nodejs nodejs-npm; \
	npm install -g dpe; \
	apk del nodejs-npm; \
	adduser -D dpe

ADD run.sh /

ENTRYPOINT [ "/run.sh" ]
CMD [ "sh" ]
