#!/bin/bash
# status.sh - Pisces Infrastructure Status Checker

# Define colors for output
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}🐟 Pisces Infrastructure Status Report ${NC}"
echo "=================================================="

echo -e "\n${YELLOW}1. 🖥️  Node Status (Spot Instances)${NC}"
kubectl get nodes

echo -e "\n${YELLOW}2. 📦 Pod Status (Gateway, Frasier Bot, Redis, Cross-Encoder)${NC}"
kubectl get pods -o custom-columns="NAME:.metadata.name,READY:.status.containerStatuses[*].ready,STATUS:.status.phase,RESTARTS:.status.containerStatuses[*].restartCount,AGE:.metadata.creationTimestamp"

echo -e "\n${YELLOW}3. 🔌 Internal Services (Routing)${NC}"
kubectl get svc

echo -e "\n${YELLOW}4. 🌐 External Gateway & Routes (Load Balancer)${NC}"
kubectl get gateway,httproute

echo -e "\n${YELLOW}5. 🛡️  Security Policies (GCPBackendPolicy)${NC}"
kubectl get gcpbackendpolicy

echo -e "\n${YELLOW}6. ⚠️  Recent Cluster Warnings (Last 5)${NC}"
kubectl get events --field-selector type=Warning --sort-by='.metadata.creationTimestamp' | tail -n 5

echo -e "\n${BLUE}==================================================${NC}"
echo -e "💡 Troubleshooting 'Connection reset by peer':"
echo -e "   1. Check if the Pods are actually 'Running' in Step 2."
echo -e "   2. Check if the Gateway has an IP address assigned in Step 4."
echo -e "   3. Run ${RED}kubectl logs -l app=pisces-gateway${NC} to check for Go app panics."