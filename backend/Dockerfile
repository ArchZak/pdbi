FROM ghcr.io/prefix-dev/pixi:0.67.2 AS build
WORKDIR /app
COPY pixi.toml pixi.lock ./
RUN pixi install --locked

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=build /app/.pixi/envs/default /app/.pixi/envs/default
COPY backend/pipeline.py backend/cloud_exports.py backend/cloud_ingest.py backend/live_sources.py backend/query_api.py ./
ENV PATH="/app/.pixi/envs/default/bin:${PATH}"
ENTRYPOINT ["/app/.pixi/envs/default/bin/python", "-m", "awslambdaric"]
CMD ["cloud_exports.handler"]
