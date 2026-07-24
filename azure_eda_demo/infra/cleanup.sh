#!/usr/bin/env bash
#
# Deletes everything provision.sh created.
# Run this when the session is over — Service Bus Standard and Event Hubs
# Standard bill per hour whether or not you are using them.

set -euo pipefail

RG="${RG:-rg-eda-demo}"

echo
echo "This will DELETE the resource group '$RG' and everything in it."
read -rp "Type the resource group name to confirm: " CONFIRM

if [[ "$CONFIRM" != "$RG" ]]; then
  echo "Names did not match. Nothing was deleted."
  exit 1
fi

az group delete --name "$RG" --yes --no-wait
echo
echo "Deletion started in the background. Check with:"
echo "  az group show --name $RG"
echo
