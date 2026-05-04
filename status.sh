#!/bin/bash
# status.sh - Pisces Infrastructure Status Tracker (Zonal Edition)

BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' 

echo -e "${BLUE}🐟 Pisces Infrastructure Zonal Status Report ${NC}"
echo "===================================================================================="

echo -e "\n${YELLOW}1. 🖥️  Node Inventory & Health${NC}"
kubectl get nodes -L topology.kubernetes.io/zone

echo -e "\n${YELLOW}2. 📦 Deployment Zonal Distribution${NC}"
# This command joins the Pod nodeName with the Node's zone label for precision
printf "%-40s %-20s %-15s %-10s\n" "POD NAME" "NODE" "ZONE" "STATUS"
echo "------------------------------------------------------------------------------------"

# Targets gateway, frasier, and cross-encoder
kubectl get pods -o json | jq -r '.items[] | select(.metadata.labels.app != null) | [
    .metadata.name, 
    .spec.nodeName, 
    (.metadata.labels["topology.kubernetes.io/zone"] // "Fetching..."), 
    .status.phase
] | @tsv' | while IFS=$'\t' read -r name node zone status; do
    # If the pod doesn't have the label, we look up the node's zone directly
    if [ "$zone" == "Fetching..." ]; then
        zone=$(kubectl get node "$node" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}' 2>/dev/null)
    fi
    printf "%-40s %-20s %-15s %-10s\n" "$name" "$node" "$zone" "$status"
done

echo -e "\n${YELLOW}3. 🔌 Internal Services${NC}"
kubectl get svc

echo -e "\n${YELLOW}4. 🌐 Gateway API & Load Balancer${NC}"
kubectl get gateway,httproute

echo -e "\n${YELLOW}5. ⚠️  Active Cluster Warnings${NC}"
kubectl get events --field-selector type=Warning --sort-by='.metadata.creationTimestamp' | tail -n 5

echo -e "\n${BLUE}====================================================================================${NC}"