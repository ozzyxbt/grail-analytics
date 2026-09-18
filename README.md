# Grail Analytics

On-chain user analytics for [Grail](https://grail.xyz) (collectible-backed tokens on Base), built entirely from public RPC logs and Grail's public registry endpoints. Produces a single self-contained HTML dashboard plus CSV exports.

What it answers:

- **Who made money** — realized + unrealized P&L per wallet (average-cost basis), overall, by venue, by token, and split by Grail-app-only / mixed / external-only usage.
- **Grail app vs routers** — every swap attributed to the front-end that sent it (Grail app, Fomo, GMGN, OKX, Telegram bots, aggregators, direct/MEV), with the real user resolved behind relayers and smart wallets.
- **Packs & exclusive mints** — gated LAUNCH / FOUNDER pack buyers (gVITALIK, gELON, gKAI, …): who redeemed, who sold, how fast, ROI.
- **Card vaulting** — physical cards entering the vault, Grail-vaulted vs user-vaulted, reconciled against Grail's reserves registry; physical redemptions traced through the claim-NFT lifecycle (shipped / pending / cancelled).
- **Snipers** — first buys within 5 minutes of a pool's first liquidity, tagged and scored.
- **GLIST wallets** — every gated-pack buyer with flipper / holder / unredeemed tags, exportable as CSV.

## How it works

```
fetch_rpc.py      eth_getLogs over mainnet.base.org (2000-block ranges, batched, adaptive splitting, resumable)
                  jobs: main (26 pools + 26 tokens + pack vault, USDC->vault), exec (executor + routers), reserves (29 reserve NFTs)
fetch_robinhood.py  Robinhood Chain (4663) tokens: Uniswap V4 PoolManager Swap/ModifyLiquidity by pool id, transfers, tx metadata; quote asset (tokenized NVDA) converted to USD
fetch_txs.py      tx.from / tx.to / selector for every swap & pack tx via publicnode (100-per-batch), resumable
decode.py         raw logs -> parquet tables: swaps, transfers, lp, inventory (redeems), usdc_vault, pack_buys, other
analysis.py       DuckDB + pandas -> out/analysis.json + CSVs
build_dashboard.py  inlines analysis.json into template.html -> out/grail_dashboard.html
```

Key on-chain facts the pipeline relies on (all on Base, chain id 8453):

| Thing | Address / detail |
|---|---|
| Token & pool registry | `https://grail.xyz/api/tokens?page=N` (26 tokens, one Uniswap V3 USDC pool each) |
| Pack registry | `https://grail.xyz/api/packs` |
| Reserve registry (physical cards) | `https://grail.xyz/api/reserves` — tokens-per-card multiplier, backed supply |
| Pack vault | `0x36B162DE23E4E809d78Fb0EAe4a2272Bc313d738` — USDC paid here; `InventoryTransferred` = card NFT redeemed into tokens |
| Pack executor | `0x4491Ac59d1e6A5D2E15a8048c2de34199e8De8dA` — purchase events (v1 `0x4adcdeed…`, v2 `0x1993895c…`) carry `keccak(pack_id)` + buyer + USDC |
| Grail router / old swap contract | `0x2cB51D6e53bA3e983A6A50d2247931C96F9D7358` / `0x94df02cC6338e6b38f60a655EA893ea0c1c2961F` |
| Grail gasless relayer | `0x8c5a2BFb1b6bbc380aBD6df6EE21679A3b6C0C93` |
| Vaulting | ERC-20 mint + reserve `BackedSupplyChanged` (`0x3789b3d374…`) in the same tx; cards = amount ÷ multiplier |
| Physical redemption | burn of a full card's tokens mints a claim NFT from the reserve contract; Grail burns it on shipment, or re-mints tokens if cancelled |
| Block time | Base timestamp = `1686789347 + 2 × block` exactly, so no block lookups are needed |

Venue classification is by `tx.to` (first contract called) with a prefix/suffix map in `analysis.py`; totals reconcile to [grailytics.xyz](https://users.grailytics.xyz) for Fomo, GMGN, BasedBot, Sigma, Banana Gun and the aggregators.

## Run it

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./refresh_config.sh          # optional: pull latest token/pack/reserve registries
./run.sh                     # full pull (~45 min on the public RPC), decode, analyse, build
open out/grail_dashboard.html
```

Every fetcher is resumable via cursor files in `data/raw/`, so re-running `./run.sh` only pulls new blocks. Raw logs and parquet are git-ignored; `out/` (dashboard, analysis.json, CSVs) is committed.

## Methodology notes

- **Trader identity**: the wallet whose net token balance moved in the swap tx, not the relayer or bundler.
- **P&L**: average-cost basis per wallet per token. Pack redemptions carry the allocated pack price; transferred-in or airdropped tokens are priced at the pool price when received. Bots (direct pool callers) are excluded from trader counts.
- **Grail-app attribution** is on-chain only (Grail contracts, settler, relayer), so it is a floor versus Grail's private user list.
- **Team wallets**: deployer, inventory contract and the team inventory wallet are excluded; other team wallets are not identifiable on-chain.

Data sources: `mainnet.base.org`, `base-rpc.publicnode.com`, `grail.xyz/api`. No API keys required.

## Hosting

`build_dashboard.py` produces two files from `out/analysis.json`: `out/grail_dashboard.html` (fragment, for the Claude artifact) and `site/index.html` (full standalone page with CSV downloads, for static hosting).

- **`.github/workflows/pages.yml`** deploys `site/` to GitHub Pages on every push that touches `out/`, the template or the build script.
- **`.github/workflows/refresh.yml`** runs every 20 minutes and on demand: pulls new blocks incrementally (raw logs and cursors are kept in the Actions cache), re-runs the analysis, commits `out/`, and redeploys.

The site carries a footer disclaimer (independent project, not affiliated with Grail Labs, not financial advice) and credits grailytics.xyz for the venue contract map.
