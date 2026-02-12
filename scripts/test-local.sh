#!/bin/bash
# test-local.sh - Run cachier tests locally with Docker for any combination of cores
# This script provides a unified interface for testing all cachier backends locally

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Configuration
MONGO_CONTAINER="cachier-test-mongo"
REDIS_CONTAINER="cachier-test-redis"
POSTGRES_CONTAINER="cachier-test-postgres"

# Default settings
VERBOSE=false
COVERAGE_REPORT="term"
KEEP_RUNNING=false
SELECTED_CORES=""
INCLUDE_LOCAL_CORES=false
TEST_FILES=""
PARALLEL=false
PARALLEL_WORKERS="auto"

# Function to print colored messages
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to print usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS] [CORES...]

Run cachier tests locally with Docker containers for external backends.

CORES:
    mongo       MongoDB backend tests
    redis       Redis backend tests
    sql         SQL (PostgreSQL) backend tests
    memory      Memory backend tests (no Docker needed)
    pickle      Pickle backend tests (no Docker needed)
    all         All backends (equivalent to: mongo redis sql memory pickle)
    external    All external backends (mongo redis sql)
    local       All local backends (memory pickle)

OPTIONS:
    -v, --verbose       Show verbose output
    -k, --keep-running  Keep containers running after tests
    -h, --html-coverage Generate HTML coverage report
    -f, --files         Specify test files to run (can be used multiple times)
    -p, --parallel      Run tests in parallel using pytest-xdist
    -w, --workers       Number of parallel workers (default: auto)
    --help              Show this help message

EXAMPLES:
    $0 mongo                    # Run only MongoDB tests
    $0 redis sql                # Run Redis and SQL tests
    $0 all                      # Run all backend tests
    $0 external -k              # Run external backends, keep containers
    $0 mongo memory -v          # Run MongoDB and memory tests verbosely
    $0 all -f tests/test_main.py -f tests/test_redis_core_coverage.py  # Run specific test files
    $0 memory pickle -p         # Run local tests in parallel
    $0 all -p -w 4             # Run all tests with 4 parallel workers

ENVIRONMENT:
    You can also set cores via CACHIER_TEST_CORES environment variable:
    CACHIER_TEST_CORES="mongo redis" $0

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -k|--keep-running)
            KEEP_RUNNING=true
            shift
            ;;
        -h|--html-coverage)
            COVERAGE_REPORT="html"
            shift
            ;;
        -f|--files)
            shift
            if [[ $# -eq 0 ]] || [[ "$1" == -* ]]; then
                print_message $RED "Error: -f/--files requires a file argument"
                usage
                exit 1
            fi
            TEST_FILES="$TEST_FILES $1"
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        -p|--parallel)
            PARALLEL=true
            shift
            ;;
        -w|--workers)
            shift
            if [[ $# -eq 0 ]] || [[ "$1" == -* ]]; then
                print_message $RED "Error: -w/--workers requires a number argument"
                usage
                exit 1
            fi
            PARALLEL_WORKERS="$1"
            shift
            ;;
        -*)
            print_message $RED "Unknown option: $1"
            usage
            exit 1
            ;;
        *)
            # This is a core name
            SELECTED_CORES="$SELECTED_CORES $1"
            shift
            ;;
    esac
done

# If no cores specified, check environment variable
if [ -z "$SELECTED_CORES" ] && [ -n "$CACHIER_TEST_CORES" ]; then
    SELECTED_CORES=$CACHIER_TEST_CORES
fi

# If still no cores specified, show usage
if [ -z "$SELECTED_CORES" ]; then
    print_message $RED "Error: No cores specified"
    usage
    exit 1
fi

# Expand core groups
expand_cores() {
    local cores=""
    for core in $1; do
        case $core in
            all)
                cores="$cores mongo redis sql memory pickle"
                ;;
            external)
                cores="$cores mongo redis sql"
                ;;
            local)
                cores="$cores memory pickle"
                ;;
            *)
                cores="$cores $core"
                ;;
        esac
    done
    # Remove duplicates
    echo "$cores" | tr ' ' '\n' | sort -u | tr '\n' ' '
}

SELECTED_CORES=$(expand_cores "$SELECTED_CORES")

# Define core to marker mappings using a function
get_markers_for_core() {
    case $1 in
        mongo) echo "mongo" ;;
        redis) echo "redis" ;;
        sql) echo "sql" ;;
        memory) echo "memory" ;;
        pickle) echo "pickle or maxage" ;;
        *) echo "$1" ;;  # Default to core name
    esac
}

# Validate cores
validate_cores() {
    local valid_cores="mongo redis sql memory pickle"
    for core in $1; do
        if ! echo "$valid_cores" | grep -qw "$core"; then
            print_message $RED "Error: Invalid core '$core'"
            print_message $YELLOW "Valid cores: mongo, redis, sql, memory, pickle"
            exit 1
        fi
    done
}

validate_cores "$SELECTED_CORES"

# Function to check if Docker is available
check_docker() {
    if ! command -v docker &> /dev/null; then
        print_message $RED "═══════════════════════════════════════════════════════════════"
        print_message $RED "ERROR: Docker is not installed!"
        print_message $RED "═══════════════════════════════════════════════════════════════"
        echo ""
        echo "This script requires Docker to run external backend tests (MongoDB, Redis, PostgreSQL)."
        echo "Please install Docker from: https://www.docker.com/products/docker-desktop"
        echo ""
        exit 1
    fi

    # Try to run docker ps and capture the actual error
    if ! docker ps > /dev/null 2>&1; then
        print_message $RED "═══════════════════════════════════════════════════════════════"
        print_message $RED "ERROR: Docker daemon is not running!"
        print_message $RED "═══════════════════════════════════════════════════════════════"
        echo ""
        echo "Docker is installed but the Docker daemon is not running."
        echo ""
        echo "To fix this:"
        echo "  • On macOS: Start Docker Desktop from Applications"
        echo "  • On Linux: Run 'sudo systemctl start docker' or 'sudo service docker start'"
        echo "  • On Windows: Start Docker Desktop from the Start Menu"
        echo ""
        echo "After starting Docker, wait a few seconds and try running this script again."
        echo ""

        # Show the actual docker error for debugging
        echo "Technical details:"
        docker ps 2>&1 | sed 's/^/  /'
        echo ""
        exit 1
    fi

    print_message $GREEN "✓ Docker is installed and running"
}

# Function to check and install dependencies
check_dependencies() {
    local missing_deps=false

    print_message $YELLOW "Checking test dependencies..."

    # Check base test requirements
    if ! python -c "import pytest" 2>/dev/null; then
        print_message $YELLOW "Installing base test requirements..."
        pip install -r tests/requirements.txt || {
            print_message $RED "Failed to install base test requirements"
            exit 1
        }
    fi

    # Check for pytest-xdist if parallel testing is requested
    if [ "$PARALLEL" = true ]; then
        if ! python -c "import xdist" 2>/dev/null; then
            print_message $YELLOW "Installing pytest-xdist for parallel testing..."
            pip install pytest-xdist || {
                print_message $RED "Failed to install pytest-xdist"
                exit 1
            }
        fi
    fi

    # Check MongoDB dependencies if testing MongoDB
    if echo "$SELECTED_CORES" | grep -qw "mongo"; then
        if ! python -c "import pymongo" 2>/dev/null; then
            print_message $YELLOW "Installing MongoDB test requirements..."
            pip install -r tests/mongodb_requirements.txt || {
                print_message $RED "Failed to install MongoDB requirements"
                exit 1
            }
        fi
    fi

    # Check Redis dependencies if testing Redis
    if echo "$SELECTED_CORES" | grep -qw "redis"; then
        if ! python -c "import redis" 2>/dev/null; then
            print_message $YELLOW "Installing Redis test requirements..."
            pip install -r tests/redis_requirements.txt || {
                print_message $RED "Failed to install Redis requirements"
                exit 1
            }
        fi
    fi

    # Check SQL dependencies if testing SQL
    if echo "$SELECTED_CORES" | grep -qw "sql"; then
        if ! python -c "import sqlalchemy" 2>/dev/null; then
            print_message $YELLOW "Installing SQL test requirements..."
            pip install -r tests/sql_requirements.txt || {
                print_message $RED "Failed to install SQL requirements"
                exit 1
            }
        fi
    fi

    print_message $GREEN "All required dependencies are installed!"
}

# MongoDB functions
start_mongodb() {
    print_message $YELLOW "Starting MongoDB container..."

    # Remove existing container if any
    docker rm -f $MONGO_CONTAINER > /dev/null 2>&1 || true

    # Start MongoDB
    if [ "$VERBOSE" = true ]; then
        docker run -d -p 27017:27017 --name $MONGO_CONTAINER mongo:latest
    else
        docker run -d -p 27017:27017 --name $MONGO_CONTAINER mongo:latest > /dev/null 2>&1
    fi

    # Wait for MongoDB to be ready
    print_message $YELLOW "Waiting for MongoDB to be ready..."
    sleep 5

    if docker exec $MONGO_CONTAINER mongosh --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
        print_message $GREEN "MongoDB is ready!"
    else
        print_message $YELLOW "MongoDB might need more time to start. Proceeding anyway..."
    fi
}

stop_mongodb() {
    if [ "$KEEP_RUNNING" = false ]; then
        print_message $YELLOW "Stopping MongoDB container..."
        docker stop $MONGO_CONTAINER > /dev/null 2>&1 || true
        docker rm $MONGO_CONTAINER > /dev/null 2>&1 || true
    else
        print_message $BLUE "MongoDB container kept running at localhost:27017"
    fi
}

test_mongodb() {
    export CACHIER_TEST_HOST=localhost
    export CACHIER_TEST_PORT=27017
    export CACHIER_TEST_VS_DOCKERIZED_MONGO=true
}

# Redis functions
start_redis() {
    print_message $YELLOW "Starting Redis container..."

    # Remove existing container if any
    docker rm -f $REDIS_CONTAINER > /dev/null 2>&1 || true

    # Start Redis
    if [ "$VERBOSE" = true ]; then
        docker run -d -p 6379:6379 --name $REDIS_CONTAINER redis:7-alpine
    else
        docker run -d -p 6379:6379 --name $REDIS_CONTAINER redis:7-alpine > /dev/null 2>&1
    fi

    # Wait for Redis to be ready
    print_message $YELLOW "Waiting for Redis to be ready..."
    sleep 2

    if docker exec $REDIS_CONTAINER redis-cli ping > /dev/null 2>&1; then
        print_message $GREEN "Redis is ready!"
    else
        print_message $YELLOW "Redis might need more time to start. Proceeding anyway..."
    fi
}

stop_redis() {
    if [ "$KEEP_RUNNING" = false ]; then
        print_message $YELLOW "Stopping Redis container..."
        docker stop $REDIS_CONTAINER > /dev/null 2>&1 || true
        docker rm $REDIS_CONTAINER > /dev/null 2>&1 || true
    else
        print_message $BLUE "Redis container kept running at localhost:6379"
    fi
}

test_redis() {
    export CACHIER_TEST_REDIS_HOST=localhost
    export CACHIER_TEST_REDIS_PORT=6379
    export CACHIER_TEST_REDIS_DB=0
    export CACHIER_TEST_VS_DOCKERIZED_REDIS=true
}

# SQL/PostgreSQL functions
start_postgres() {
    print_message $YELLOW "Starting PostgreSQL container..."

    # Remove existing container if any
    docker rm -f $POSTGRES_CONTAINER > /dev/null 2>&1 || true

    # Start PostgreSQL
    if [ "$VERBOSE" = true ]; then
        docker run -d \
            -e POSTGRES_USER=testuser \
            -e POSTGRES_PASSWORD=testpass \
            -e POSTGRES_DB=testdb \
            -p 5432:5432 \
            --name $POSTGRES_CONTAINER \
            postgres:15
    else
        docker run -d \
            -e POSTGRES_USER=testuser \
            -e POSTGRES_PASSWORD=testpass \
            -e POSTGRES_DB=testdb \
            -p 5432:5432 \
            --name $POSTGRES_CONTAINER \
            postgres:15 > /dev/null 2>&1
    fi

    # Wait for PostgreSQL to be ready
    print_message $YELLOW "Waiting for PostgreSQL to be ready..."
    sleep 5

    if docker exec $POSTGRES_CONTAINER pg_isready -U testuser > /dev/null 2>&1; then
        print_message $GREEN "PostgreSQL is ready!"
    else
        print_message $YELLOW "PostgreSQL might need more time to start. Proceeding anyway..."
    fi
}

stop_postgres() {
    if [ "$KEEP_RUNNING" = false ]; then
        print_message $YELLOW "Stopping PostgreSQL container..."
        docker stop $POSTGRES_CONTAINER > /dev/null 2>&1 || true
        docker rm $POSTGRES_CONTAINER > /dev/null 2>&1 || true
    else
        print_message $BLUE "PostgreSQL container kept running at localhost:5432"
    fi
}

test_sql() {
    export SQLALCHEMY_DATABASE_URL="postgresql://testuser:testpass@localhost:5432/testdb"
}

# Main execution
main() {
    print_message $GREEN "=== Cachier Local Testing ==="
    print_message $BLUE "Selected cores: $SELECTED_CORES"

    # Check and install dependencies
    check_dependencies

    # Check if we need Docker, and if we should run serial pickle tests
    needs_docker=false
    run_serial_local_tests=false
    for core in $SELECTED_CORES; do
        case $core in
            mongo|redis|sql)
                needs_docker=true
                ;;
        esac
        case $core in
            pickle|all)
                run_serial_local_tests=true
                ;;
        esac
    done

    if [ "$needs_docker" = true ]; then
        check_docker
    fi

    # Track which containers we started
    STARTED_CONTAINERS=""

    # Start required services
    for core in $SELECTED_CORES; do
        case $core in
            mongo)
                start_mongodb
                STARTED_CONTAINERS="$STARTED_CONTAINERS mongo"
                ;;
            redis)
                start_redis
                STARTED_CONTAINERS="$STARTED_CONTAINERS redis"
                ;;
            sql)
                start_postgres
                STARTED_CONTAINERS="$STARTED_CONTAINERS sql"
                ;;
        esac
    done

    # Set up cleanup trap
    cleanup() {
        for container in $STARTED_CONTAINERS; do
            case $container in
                mongo) stop_mongodb ;;
                redis) stop_redis ;;
                sql) stop_postgres ;;
            esac
        done
    }
    trap cleanup EXIT

    # Run tests
    print_message $YELLOW "Running tests..."

    # Build pytest marker expression
    pytest_markers=""
    for core in $SELECTED_CORES; do
        # Get the markers for this core
        core_markers=$(get_markers_for_core "$core")

        if [ -z "$pytest_markers" ]; then
            pytest_markers="$core_markers"
        else
            # Add parentheses around multi-part markers for proper precedence
            if [[ "$core_markers" == *" or "* ]]; then
                pytest_markers="$pytest_markers or ($core_markers)"
            else
                pytest_markers="$pytest_markers or $core_markers"
            fi
        fi

        # Set environment variables for each core
        case $core in
            mongo) test_mongodb ;;
            redis) test_redis ;;
            sql) test_sql ;;
        esac
    done
    pytest_markers="$pytest_markers and not seriallocal"

    # Run pytest
    # Build pytest command
    PYTEST_CMD="pytest"
    # and the specific pytest command for running serial pickle tests
    SERIAL_PYTEST_CMD="pytest -m seriallocal -n0"

    # Add test files if specified
    if [ -n "$TEST_FILES" ]; then
        PYTEST_CMD="$PYTEST_CMD $TEST_FILES"
        print_message $BLUE "Test files specified: $TEST_FILES"
        # and turn off serial local tests, so we run only selected files
        run_serial_local_tests=false
    fi

    # Add markers if needed (only if no specific test files were given)
    if [ -z "$TEST_FILES" ]; then
        # Check if we selected all cores - if so, run all tests without marker filtering
        all_cores="memory mongo pickle redis sql"
        selected_sorted=$(echo "$SELECTED_CORES" | tr ' ' '\n' | sort | tr '\n' ' ' | xargs)
        all_sorted=$(echo "$all_cores" | tr ' ' '\n' | sort | tr '\n' ' ' | xargs)

        if [ "$selected_sorted" != "$all_sorted" ]; then
            PYTEST_CMD="$PYTEST_CMD -m \"$pytest_markers\""
        else
            print_message $BLUE "Running all tests without markers since all cores are selected"
            PYTEST_CMD="$PYTEST_CMD -m \"not seriallocal\""
            run_serial_local_tests=true
        fi
    else
        # When test files are specified, still apply markers if not running all cores
        all_cores="memory mongo pickle redis sql"
        selected_sorted=$(echo "$SELECTED_CORES" | tr ' ' '\n' | sort | tr '\n' ' ' | xargs)
        all_sorted=$(echo "$all_cores" | tr ' ' '\n' | sort | tr '\n' ' ' | xargs)

        if [ "$selected_sorted" != "$all_sorted" ]; then
            PYTEST_CMD="$PYTEST_CMD -m \"$pytest_markers\""
        fi
    fi

    # Add verbose flag if needed
    if [ "$VERBOSE" = true ]; then
        PYTEST_CMD="$PYTEST_CMD -v"
        SERIAL_PYTEST_CMD="$SERIAL_PYTEST_CMD -v"
    fi

    # Add parallel testing options if requested
    if [ "$PARALLEL" = true ]; then
        PYTEST_CMD="$PYTEST_CMD -n $PARALLEL_WORKERS"

        # Show parallel testing info
        if [ "$PARALLEL_WORKERS" = "auto" ]; then
            print_message $BLUE "Running tests in parallel with automatic worker detection"
        else
            print_message $BLUE "Running tests in parallel with $PARALLEL_WORKERS workers"
        fi

        # Special note for pickle tests
        if echo "$SELECTED_CORES" | grep -qw "pickle"; then
            print_message $YELLOW "Note: Pickle tests will use isolated cache directories for parallel safety"
        fi
    fi

    # Add coverage options
    PYTEST_CMD="$PYTEST_CMD --cov=cachier --cov-report=$COVERAGE_REPORT"
    SERIAL_PYTEST_CMD="$SERIAL_PYTEST_CMD --cov=cachier --cov-report=$COVERAGE_REPORT --cov-append"

    # Print and run the command
    print_message $BLUE "Running: $PYTEST_CMD"
    eval $PYTEST_CMD

    if [ "$run_serial_local_tests" = true ]; then
        print_message $BLUE "Running serial local tests (pickle, memory) with: $SERIAL_PYTEST_CMD"
        eval $SERIAL_PYTEST_CMD
    else
        print_message $BLUE "Skipping serial local tests (pickle, memory) since not requested"
    fi

    TEST_EXIT_CODE=$?

    if [ $TEST_EXIT_CODE -eq 0 ]; then
        print_message $GREEN "All tests passed!"
    else
        print_message $RED "Some tests failed. Exit code: $TEST_EXIT_CODE"
    fi

    # Exit with test status
    exit $TEST_EXIT_CODE
}

# Run main function
main
