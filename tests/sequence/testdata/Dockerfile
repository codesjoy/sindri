FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY artifacts/sequence/sequence /app/sequence
EXPOSE 19010
ENTRYPOINT ["/app/sequence"]
