#!/bin/bash
# ProStaff Scraper - Production Deployment Script
# Deploys competitive-focused ETL pipeline with rate limiting

set -e  # Exit on error

echo "=========================================="
echo "ProStaff Scraper - Production Deployment"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if running as root (not recommended)
if [ "$EUID" -eq 0 ]; then
   echo -e "${YELLOW}Warning: Running as root is not recommended${NC}"
fi

# 1. Check environment variables
echo -e "\n${GREEN}Step 1: Checking environment variables...${NC}"

if [ ! -f .env ]; then
    echo -e "${RED}ERROR: .env file not found!${NC}"
    echo "Creating from template..."
    cp .env.production.example .env
    echo -e "${YELLOW}Please edit .env with your API keys before continuing${NC}"
    exit 1
fi

# Source .env file
export $(cat .env | grep -v '^#' | xargs)

# Check required variables
if [ -z "$RIOT_API_KEY" ]; then
    echo -e "${RED}ERROR: RIOT_API_KEY not set in .env${NC}"
    exit 1
fi

if [ -z "$ESPORTS_API_KEY" ]; then
    echo -e "${RED}ERROR: ESPORTS_API_KEY not set in .env${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Environment variables configured${NC}"

# 2. Create necessary directories
echo -e "\n${GREEN}Step 2: Creating directories...${NC}"

mkdir -p data/competitive
mkdir -p data/cache/matches
mkdir -p data/cache/timelines
mkdir -p logs
mkdir -p elasticsearch/config

echo -e "${GREEN}✓ Directories created${NC}"

# 3. Fix import paths in pipeline
echo -e "\n${GREEN}Step 3: Fixing import paths...${NC}"

# Fix the import issue in cblol.py
sed -i 's/from scraper\.providers/from providers/g' pipelines/cblol.py
sed -i 's/from scraper\.indexers/from indexers/g' pipelines/cblol.py

echo -e "${GREEN}✓ Import paths fixed${NC}"

# 4. Test Elasticsearch connection (if running)
echo -e "\n${GREEN}Step 4: Testing connections...${NC}"

# Check if Elasticsearch is accessible
if [ ! -z "$ELASTICSEARCH_URL" ]; then
    if curl -s "$ELASTICSEARCH_URL/_cluster/health" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Elasticsearch is accessible at $ELASTICSEARCH_URL${NC}"
    else
        echo -e "${YELLOW}⚠ Elasticsearch not accessible. Will use cache-only mode.${NC}"
    fi
else
    ELASTICSEARCH_URL="http://localhost:9200"
    echo -e "${YELLOW}Using default Elasticsearch URL: $ELASTICSEARCH_URL${NC}"
fi

# 5. Install Python dependencies
echo -e "\n${GREEN}Step 5: Installing dependencies...${NC}"

# Check if we're in a virtual environment
if [ -z "$VIRTUAL_ENV" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
fi

pip install --upgrade pip
pip install -r requirements.txt

echo -e "${GREEN}✓ Dependencies installed${NC}"

# 6. Run competitive pipeline test
echo -e "\n${GREEN}Step 6: Testing competitive pipeline...${NC}"

echo "Testing with 5 CBLOL matches..."
python etl/competitive_pipeline.py --leagues CBLOL --limit 5

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Pipeline test successful${NC}"
else
    echo -e "${RED}✗ Pipeline test failed${NC}"
    exit 1
fi

# 7. Setup systemd service (optional)
echo -e "\n${GREEN}Step 7: Systemd service setup...${NC}"

read -p "Do you want to create a systemd service? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    cat > /tmp/prostaff-scraper.service <<EOF
[Unit]
Description=ProStaff Competitive Scraper
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$(pwd)
Environment="PATH=$(pwd)/venv/bin:/usr/bin"
ExecStart=$(pwd)/venv/bin/python etl/competitive_pipeline.py --daemon --leagues CBLOL LCS LEC --interval 1
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

    sudo mv /tmp/prostaff-scraper.service /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable prostaff-scraper
    echo -e "${GREEN}✓ Systemd service created${NC}"
    echo "Start with: sudo systemctl start prostaff-scraper"
    echo "Check status: sudo systemctl status prostaff-scraper"
    echo "View logs: sudo journalctl -u prostaff-scraper -f"
fi

# 8. Docker deployment (if using Coolify)
echo -e "\n${GREEN}Step 8: Docker deployment...${NC}"

if command -v docker &> /dev/null; then
    echo "Docker detected. Building production image..."
    docker build -f Dockerfile.production -t prostaff-scraper:latest .
    echo -e "${GREEN}✓ Docker image built${NC}"

    echo -e "\n${YELLOW}For Coolify deployment:${NC}"
    echo "1. Push this repository to GitHub"
    echo "2. In Coolify, create new 'Docker Compose' application"
    echo "3. Point to docker-compose.production.yml"
    echo "4. Set environment variables in Coolify UI"
    echo "5. Deploy!"
else
    echo -e "${YELLOW}Docker not found. Skipping container build.${NC}"
fi

# 9. Final summary
echo -e "\n${GREEN}=========================================="
echo "DEPLOYMENT COMPLETE!"
echo "==========================================${NC}"

echo -e "\n${GREEN}✓ What's been configured:${NC}"
echo "  • Rate-limited Riot API client with caching"
echo "  • Competitive-only match extraction"
echo "  • Elasticsearch indexing ready"
echo "  • Production logging enabled"
echo "  • Background daemon mode available"

echo -e "\n${GREEN}Next steps:${NC}"
echo "1. Deploy Elasticsearch via Coolify UI (Services > Elasticsearch with Kibana)"
echo "2. Update ELASTICSEARCH_URL in .env with Coolify service URL"
echo "3. Start the scraper:"
echo "   - Manual: python etl/competitive_pipeline.py --leagues CBLOL"
echo "   - Daemon: python etl/competitive_pipeline.py --daemon --leagues CBLOL LCS LEC"
echo "   - Docker: docker-compose -f docker-compose.production.yml up -d"

echo -e "\n${GREEN}Monitoring:${NC}"
echo "  • Logs: tail -f logs/competitive_pipeline.log"
echo "  • Kibana: http://your-server:5601 (after Elasticsearch deploy)"
echo "  • API Health: curl http://localhost:8000/health"

echo -e "\n${YELLOW}⚠ Important:${NC}"
echo "  • Monitor rate limits in logs"
echo "  • Cache is at data/cache/ (can grow large)"
echo "  • Backup data/competitive/ regularly"

echo -e "\n${GREEN}Happy scraping competitive matches! 🎮${NC}"