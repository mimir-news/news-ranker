FROM czarsimon/godep:1.11.2-alpine3.8 as build

# Copy source
WORKDIR /go/src/news-ranker
COPY . .

# Install dependencies
RUN dep ensure

# Build application
WORKDIR /go/src/news-ranker/cmd
RUN go build

FROM alpine:3.8 as run
WORKDIR /opt/app
COPY --from=build /go/src/news-ranker/cmd/cmd news-ranker
COPY cmd/migrations migrations
CMD ["./news-ranker"]
