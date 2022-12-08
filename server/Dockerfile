FROM golang:alpine

# Move to working directory /app
WORKDIR /app


# Copy the code into the container
COPY . .


# Download dependencies using go mod
RUN go mod download


# Build the application's binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o server .


# Command to run the application when starting the container
CMD ["/app/server"]
