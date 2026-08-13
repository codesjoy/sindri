# syntax=docker/dockerfile:1
# Copyright 2026 Codesjoy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:1.26.4 AS build

WORKDIR /src

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /out/goose github.com/pressly/goose/v3/cmd/goose

FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=build /out/goose /app/goose
COPY migrations/sequence /app/migrations

ENTRYPOINT ["/app/goose"]
CMD ["up"]
