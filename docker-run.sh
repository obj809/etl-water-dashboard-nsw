#!/bin/bash
# docker-run.sh - Helper script for running the ETL pipeline in Docker

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_usage() {
    echo "Usage: ./docker-run.sh [command]"
    echo ""
    echo "Commands:"
    echo "  build         Build the Docker image"
    echo "  down          Stop containers"
    echo "  pipeline      Run full ETL pipeline"
    echo "  pipeline-no-tests  Run pipeline without tests"
    echo "  extract       Run extract stage only"
    echo "  transform     Run transform stage only"
    echo "  load          Run load stage only"
    echo "  test          Run all tests"
    echo "  shell         Open shell in container"
    echo "  logs          View container logs"
    echo "  prod          Run pipeline in production mode (external DB)"
    echo ""
    echo "Note: Requires local MySQL running on host machine"
    echo ""
    echo "Examples:"
    echo "  ./docker-run.sh build"
    echo "  ./docker-run.sh pipeline"
}

case "$1" in
    build)
        echo -e "${GREEN}Building Docker image...${NC}"
        docker-compose build
        ;;
    down)
        echo -e "${YELLOW}Stopping local environment...${NC}"
        docker-compose down
        ;;
    pipeline)
        echo -e "${GREEN}Running full ETL pipeline...${NC}"
        docker-compose run --rm etl
        ;;
    pipeline-no-tests)
        echo -e "${GREEN}Running ETL pipeline without tests...${NC}"
        docker-compose run --rm etl --no-tests
        ;;
    extract)
        echo -e "${GREEN}Running extract stage...${NC}"
        docker-compose run --rm etl --stage extract
        ;;
    transform)
        echo -e "${GREEN}Running transform stage...${NC}"
        docker-compose run --rm etl --stage transform
        ;;
    load)
        echo -e "${GREEN}Running load stage...${NC}"
        docker-compose run --rm etl --stage load
        ;;
    test)
        echo -e "${GREEN}Running tests...${NC}"
        docker-compose run --rm etl sh -c "pytest tests/ -v"
        ;;
    shell)
        echo -e "${GREEN}Opening shell in container...${NC}"
        docker-compose run --rm --entrypoint /bin/bash etl
        ;;
    logs)
        docker-compose logs -f
        ;;
    prod)
        echo -e "${GREEN}Running pipeline in production mode...${NC}"
        docker-compose -f docker-compose.prod.yml run --rm etl
        ;;
    *)
        print_usage
        exit 1
        ;;
esac
