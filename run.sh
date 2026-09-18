#!/bin/bash
# Full pipeline: pull logs -> tx metadata -> decode -> analyse -> build dashboard.
# Usage: ./run.sh [head_block]   (defaults to latest block from the Base RPC)
set -e
cd "$(dirname "$0")"
PY=${PY:-.venv/bin/python}
HEAD=${1:-$(curl -s -X POST https://mainnet.base.org -H 'content-type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' | python3 -c 'import json,sys; print(int(json.load(sys.stdin)["result"],16))')}
echo "head block $HEAD"
mkdir -p data/raw data/parquet out
$PY fetch_rpc.py main $HEAD
$PY fetch_rpc.py exec $HEAD
$PY fetch_rpc.py reserves $HEAD
$PY fetch_robinhood.py        # tokens on Robinhood Chain (Uniswap V4), own RPC + cursor
$PY decode.py
$PY fetch_txs.py
$PY analysis.py
$PY build_dashboard.py
echo "done -> out/grail_dashboard.html"
