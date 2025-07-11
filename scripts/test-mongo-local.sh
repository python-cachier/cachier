#!/bin/bash
# test-mongo-local.sh - Run MongoDB tests locally with Docker
# This script replicates the CI MongoDB testing environment locally

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
CONTAINER_NAME="cachier-test-mongo"
MONGODB_PORT=27017
MONGODB_IMAGE="mongo:latest"
WAIT_TIME=5

# Parse command line arguments
KEEP_RUNNING=false
VERBOSE=false
COVERAGE_REPORT="term"
TEST_MODE="mongo"  # Default to mongo only

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --keep-running) KEEP_RUNNING=true ;;
        --verbose|-v) VERBOSE=true ;;
        --coverage-html) COVERAGE_REPORT="html" ;;
        --mode)
            if [[ "$2" == "also-local" ]]; then
                TEST_MODE="also-local"
            else
                echo "Unknown mode: $2. Use 'also-local' to include memory, pickle, and maxage tests."
                exit 1
            fi
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --keep-running    Keep MongoDB container running after tests"
            echo "  --verbose, -v     Show verbose output"
            echo "  --coverage-html   Generate HTML coverage report"
            echo "  --mode also-local Include memory, pickle, and maxage tests"
            echo "  --help, -h        Show this help message"
            exit 0
            ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# Function to print colored messages
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to check if Docker is available
check_docker() {
    if ! command -v docker &> /dev/null; then
        print_message $RED "Error: Docker is required but not installed."
        echo "Please install Docker from: https://www.docker.com/products/docker-desktop"
        exit 1
    fi

    if ! docker ps &> /dev/null; then
        print_message $RED "Error: Docker daemon is not running."
        echo "Please start Docker and try again."
        exit 1
    fi
}

# Function to check if port is available
check_port() {
    if lsof -Pi :$MONGODB_PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
        print_message $RED "Error: Port $MONGODB_PORT is already in use."
        echo "Please stop the service using this port or use a different port."
        exit 1
    fi
}

# Function to start MongoDB container
start_mongodb() {
    print_message $YELLOW "Starting MongoDB container..."

    # Check if container already exists
    if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        print_message $YELLOW "Removing existing container..."
        docker rm -f $CONTAINER_NAME > /dev/null 2>&1
    fi

    # Start new container
    if [ "$VERBOSE" = true ]; then
        docker run -d -p $MONGODB_PORT:27017 --name $CONTAINER_NAME $MONGODB_IMAGE
    else
        docker run -d -p $MONGODB_PORT:27017 --name $CONTAINER_NAME $MONGODB_IMAGE > /dev/null 2>&1
    fi

    if [ $? -eq 0 ]; then
        print_message $GREEN "MongoDB container started successfully."
    else
        print_message $RED "Failed to start MongoDB container."
        exit 1
    fi

    # Wait for MongoDB to be ready
    print_message $YELLOW "Waiting for MongoDB to be ready..."
    sleep $WAIT_TIME

    # Verify MongoDB is responding
    if docker exec $CONTAINER_NAME mongosh --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
        print_message $GREEN "MongoDB is ready!"
    else
        print_message $YELLOW "MongoDB might need more time to start. Proceeding anyway..."
    fi
}

# Function to run tests
run_tests() {
    print_message $YELLOW "Running MongoDB tests..."
    echo $PWD

    # Set environment variables
    export CACHIER_TEST_HOST=localhost
    export CACHIER_TEST_PORT=$MONGODB_PORT
    export CACHIER_TEST_VS_DOCKERIZED_MONGO=true

    # Run pytest with coverage
    if [ "$TEST_MODE" = "also-local" ]; then
        print_message $YELLOW "Running MongoDB tests with local core tests (memory, pickle, maxage)..."
        if [ "$VERBOSE" = true ]; then
            pytest -v -m "mongo or memory or pickle or maxage" \
                --cov=cachier --cov-report=$COVERAGE_REPORT
        else
            pytest -m "mongo or memory or pickle or maxage" \
                --cov=cachier --cov-report=$COVERAGE_REPORT
        fi
    else
        print_message $YELLOW "Running MongoDB tests only..."
        if [ "$VERBOSE" = true ]; then
            pytest -v -m "mongo" \
                --cov=cachier --cov-report=$COVERAGE_REPORT
        else
            pytest -m "mongo" \
                --cov=cachier --cov-report=$COVERAGE_REPORT
        fi
    fi

    # Capture test exit code
    TEST_EXIT_CODE=$?

    if [ $TEST_EXIT_CODE -eq 0 ]; then
        print_message $GREEN "All tests passed!"
    else
        print_message $RED "Some tests failed. Exit code: $TEST_EXIT_CODE"
    fi

    return $TEST_EXIT_CODE
}

# Function to cleanup
cleanup() {
    if [ "$KEEP_RUNNING" = true ]; then
        print_message $YELLOW "MongoDB container kept running at localhost:$MONGODB_PORT"
        echo "To stop it manually, run: docker stop $CONTAINER_NAME && docker rm $CONTAINER_NAME"
    else
        print_message $YELLOW "Cleaning up..."
        docker stop $CONTAINER_NAME > /dev/null 2>&1
        docker rm $CONTAINER_NAME > /dev/null 2>&1
        print_message $GREEN "Cleanup complete."
    fi
}

# Main execution
main() {
    print_message $GREEN "=== Cachier MongoDB Local Testing ==="

    # Check prerequisites
    check_docker
    check_port

    # Set trap for cleanup on exit
    trap cleanup EXIT

    # Start MongoDB
    start_mongodb

    # Run tests
    run_tests
    TEST_RESULT=$?

    # Exit with test status
    exit $TEST_RESULT
}

# Run main function
main
