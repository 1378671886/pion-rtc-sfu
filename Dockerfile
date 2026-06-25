FROM golang:1.21-alpine AS build

RUN apk add --no-cache gcc musl-dev

WORKDIR /app
COPY go.mod main.go ./
RUN go mod tidy && go build -o sfu .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /app/sfu .
EXPOSE 8080
CMD ["./sfu"]
