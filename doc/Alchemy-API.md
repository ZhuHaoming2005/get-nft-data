# NFT Metadata By Token ID [Batch]

POST https://eth-mainnet.g.alchemy.com/nft/v3/{apiKey}/getNFTMetadataBatch

getNFTMetadataBatch - Retrieves metadata for up to 100 specified NFT contracts in a single request. This endpoint is supported on Ethereum and many L2s, including Polygon, Arbitrum, Optimism, Base, World Chain and more. See the full list of supported networks [here](https://dashboard.alchemy.com/chains).

Reference: https://www.alchemy.com/docs/reference/nft-api-endpoints/nft-api-endpoints/nft-metadata-endpoints/get-nft-metadata-batch-v-3

## Code Examples

### Python

```python
import requests

url = "https://eth-mainnet.g.alchemy.com/nft/v3/docs-demo/getNFTMetadataBatch"

payload = {
    "tokens": [
        {
            "contractAddress": "0xe785E82358879F061BC3dcAC6f0444462D4b5330",
            "tokenId": "44",
            "tokenType": "string"
        }
    ],
    "tokenUriTimeoutInMs": 1,
    "refreshCache": False
}
headers = {"Content-Type": "application/json"}

response = requests.post(url, json=payload, headers=headers)

print(response.text)
```

