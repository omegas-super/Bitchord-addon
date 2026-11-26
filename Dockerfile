# Self-hosting / VPS image. Mirrors rairulyle/bitchord-selfhosted-addon:
# non-root user 65532, /data for the config, PORT configurable.
FROM node:22-alpine

WORKDIR /app
COPY package.json server.js setup.html ./
RUN chown -R 65532:65532 /app && mkdir -p /data && chown -R 65532:65532 /data

USER 65532
ENV PORT=8080 DATA_DIR=/data NODE_ENV=production
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://127.0.0.1:${PORT}/health || exit 1

CMD ["node", "server.js"]
