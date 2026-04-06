Get trending tokens

# Get trending tokens

Get trending tokens based on OpenSea's trending score algorithm. Returns tokens with high momentum including memecoins and newly popular assets.

# OpenAPI definition

```json
{
  "openapi": "3.1.0",
  "info": {
    "title": "OpenSea API",
    "description": "The API for OpenSea",
    "contact": {
      "name": "OpenSea",
      "url": "https://www.opensea.io",
      "email": "contact@opensea.io"
    },
    "version": "2.0.0"
  },
  "servers": [
    {
      "url": "https://api.opensea.io",
      "description": "Production server"
    }
  ],
  "security": [
    {
      "ApiKeyAuth": []
    }
  ],
  "tags": [
    {
      "name": "Token Endpoints",
      "description": "Token endpoints for discovering tokens, getting token details, and swapping tokens"
    }
  ],
  "paths": {
    "/api/v2/tokens/trending": {
      "get": {
        "tags": [
          "Token Endpoints"
        ],
        "summary": "Get trending tokens",
        "description": "Get trending tokens based on OpenSea's trending score algorithm. Returns tokens with high momentum including memecoins and newly popular assets.",
        "operationId": "get_trending_tokens",
        "parameters": [
          {
            "name": "limit",
            "in": "query",
            "description": "Number of results to return (default: 20, max: 100)",
            "required": false,
            "schema": {
              "type": "integer",
              "format": "int32",
              "default": 20
            },
            "example": 20
          },
          {
            "name": "chains",
            "in": "query",
            "description": "Filter by blockchain(s)",
            "required": false,
            "schema": {
              "type": "array",
              "items": {
                "$ref": "#/components/schemas/ChainIdentifier"
              }
            },
            "example": "ethereum"
          },
          {
            "name": "cursor",
            "in": "query",
            "description": "Pagination cursor for next page",
            "required": false,
            "schema": {
              "type": "string"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "OK",
            "content": {
              "*/*": {
                "schema": {
                  "$ref": "#/components/schemas/TokenPaginatedResponse"
                }
              }
            }
          },
          "400": {
            "$ref": "#/components/responses/BadRequest"
          },
          "401": {
            "description": "Unauthorized",
            "content": {
              "*/*": {
                "schema": {
                  "$ref": "#/components/schemas/V1ErrorWrapper"
                }
              }
            }
          },
          "404": {
            "description": "Not Found",
            "content": {
              "*/*": {
                "schema": {
                  "$ref": "#/components/schemas/V1ErrorWrapper"
                }
              }
            }
          },
          "500": {
            "$ref": "#/components/responses/InternalError"
          }
        }
      }
    }
  },
  "components": {
    "schemas": {
      "ChainIdentifier": {
        "type": "string",
        "default": "ethereum",
        "description": "Blockchain chain identifier. Use the chain slug (e.g., 'ethereum', 'polygon', 'arbitrum', 'optimism', 'base')",
        "enum": [
          "blast",
          "base",
          "ethereum",
          "zora",
          "arbitrum",
          "sei",
          "avalanche",
          "polygon",
          "optimism",
          "ape_chain",
          "flow",
          "b3",
          "soneium",
          "ronin",
          "bera_chain",
          "solana",
          "shape",
          "unichain",
          "gunzilla",
          "abstract",
          "animechain",
          "hyperevm",
          "somnia",
          "monad",
          "hyperliquid"
        ],
        "example": "ethereum"
      },
      "V1ErrorWrapper": {
        "type": "object",
        "properties": {
          "errors": {
            "type": "array",
            "items": {
              "type": "string"
            }
          }
        },
        "required": [
          "errors"
        ]
      },
      "TokenPaginatedResponse": {
        "type": "object",
        "description": "Paginated list of tokens",
        "properties": {
          "tokens": {
            "type": "array",
            "description": "List of tokens",
            "items": {
              "$ref": "#/components/schemas/TokenResponse"
            }
          },
          "next": {
            "type": "string",
            "description": "Cursor for the next page of results"
          }
        },
        "required": [
          "tokens"
        ]
      },
      "TokenResponse": {
        "type": "object",
        "description": "A token with summary market data",
        "properties": {
          "address": {
            "type": "string",
            "description": "The contract address of the token",
            "example": 9.175510568426712e+47
          },
          "chain": {
            "type": "string",
            "description": "The blockchain the token is on",
            "example": "ethereum"
          },
          "name": {
            "type": "string",
            "description": "The display name of the token",
            "example": "USD Coin"
          },
          "symbol": {
            "type": "string",
            "description": "The ticker symbol of the token",
            "example": "USDC"
          },
          "image_url": {
            "type": "string",
            "description": "URL of the token's image"
          },
          "usd_price": {
            "type": "string",
            "description": "Current price in USD",
            "example": 1
          },
          "decimals": {
            "type": "integer",
            "format": "int32",
            "description": "Number of decimal places",
            "example": 6
          },
          "market_cap_usd": {
            "type": "number",
            "format": "double",
            "description": "Market capitalization in USD"
          },
          "volume_24h": {
            "type": "number",
            "format": "double",
            "description": "24-hour trading volume in USD"
          },
          "price_change_24h": {
            "type": "number",
            "format": "double",
            "description": "Price change percentage over the last 24 hours"
          },
          "opensea_url": {
            "type": "string",
            "description": "URL to the token page on OpenSea"
          }
        },
        "required": [
          "address",
          "chain",
          "decimals",
          "name",
          "opensea_url",
          "symbol",
          "usd_price"
        ]
      }
    },
    "responses": {
      "BadRequest": {
        "description": "For error reasons, review the response data."
      },
      "InternalError": {
        "description": "Internal server error. Please open a support ticket so OpenSea can investigate."
      }
    },
    "securitySchemes": {
      "ApiKeyAuth": {
        "type": "apiKey",
        "description": "API key required for authentication",
        "name": "x-api-key",
        "in": "header"
      }
    }
  }
}
```