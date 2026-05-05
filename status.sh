#!/bin/bash
# status.sh - Pisces Infrastructure Status Tracker (Zonal & GPU Edition)

BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' 

echo -e "${BLUE}🐟 Pisces Infrastructure Status Report ${NC}"
echo "===================================================================================="

echo -e "\n${YELLOW}1. 🖥️  Node Inventory & Health${NC}"
kubectl get nodes -L topology.kubernetes.io/zone

echo -e "\n${YELLOW}2. 🧠 GPU Hardware & Capacity${NC}"
# Targets nodes with GPUs to verify accelerator type, capacity, and allocatable limits
kubectl get nodes -l cloud.google.com/gke-accelerator -o custom-columns="NODE:.metadata.name,ZONE:.metadata.labels.topology\.kubernetes\.io/zone,GPU_TYPE:.metadata.labels.cloud\.google\.com/gke-accelerator,CAPACITY:.status.capacity.nvidia\.com/gpu,ALLOCATABLE:.status.allocatable.nvidia\.com/gpu"

echo -e "\n${YELLOW}3. 🤖 vLLM Engine Status${NC}"
# Shows top-level deployment rollout status
kubectl get deployment vllm-server
echo ""
# Shows granular pod status (specifically tracking readiness and crash loops)
kubectl get pods -l app=vllm -o custom-columns="NAME:.metadata.name,STATUS:.status.phase,READY:.status.containerStatuses[0].ready,RESTARTS:.status.containerStatuses[0].restartCount,AGE:.metadata.creationTimestamp,NODE:.spec.nodeName"

echo -e "\n${YELLOW}4. 📦 Deployment Zonal Distribution${NC}"
# This command joins the Pod nodeName with the Node's zone label for precision
printf "%-40s %-20s %-15s %-10s\n" "POD NAME" "NODE" "ZONE" "STATUS"
echo "------------------------------------------------------------------------------------"

# Targets all labeled apps (including gateway, frasier, cross-encoder, and vllm)
kubectl get pods -o json | jq -r '.items[] | select(.metadata.labels.app != null) | [
    .metadata.name, 
    .spec.nodeName, 
    (.metadata.labels["topology.kubernetes.io/zone"] // "Fetching..."), 
    .status.phase
] | @tsv' | while IFS=$'\t' read -r name node zone status; do
    # If the pod doesn't have the label and is scheduled, we look up the node's zone directly
    if [ "$zone" == "Fetching..." ] && [ "$node" != "null" ]; then
        zone=$(kubectl get node "$node" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}' 2>/dev/null)
    fi
    printf "%-40s %-20s %-15s %-10s\n" "$name" "$node" "$zone" "$status"
done

echo -e "\n${YELLOW}5. 🔌 Internal Services${NC}"
kubectl get svc

echo -e "\n${YELLOW}6. 🌐 Gateway API & Load Balancer${NC}"
kubectl get gateway,httproute

echo -e "\n${YELLOW}7. ⚠️  Active Cluster Warnings${NC}"
kubectl get events --field-selector type=Warning --sort-by='.metadata.creationTimestamp' | tail -n 5

echo -e "\n${BLUE}====================================================================================${NC}"