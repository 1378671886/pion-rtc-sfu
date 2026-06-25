FROM golang:1.21-alpine AS build

# 使用阿里云镜像加速
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
RUN apk add --no-cache gcc musl-dev

ENV GOPROXY=https://goproxy.cn,direct

WORKDIR /app
COPY go.mod main.go ./
RUN go mod tidy && go build -o sfu .

FROM alpine:3.19
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /app/sfu .
EXPOSE 8080
CMD ["./sfu"]
