Validate NFT metadata

# Validate NFT metadata

Fetch and validate NFT metadata directly from the blockchain without using cached data. Returns both original and processed (SeaDN) URLs to show how the metadata would be ingested. This endpoint does not persist any data.

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
      "name": "NFT Endpoints",
      "description": "NFT endpoints to retrieve individual NFTs, collections, and related information"
    }
  ],
  "paths": {
    "/api/v2/chain/{chain}/contract/{address}/nfts/{identifier}/validate-metadata": {
      "post": {
        "tags": [
          "NFT Endpoints"
        ],
        "summary": "Validate NFT metadata",
        "description": "Fetch and validate NFT metadata directly from the blockchain without using cached data. Returns both original and processed (SeaDN) URLs to show how the metadata would be ingested. This endpoint does not persist any data.",
        "operationId": "validate_nft_metadata",
        "parameters": [
          {
            "name": "chain",
            "in": "path",
            "description": "The blockchain on which to filter the results",
            "required": true,
            "schema": {
              "$ref": "#/components/schemas/ChainIdentifier"
            },
            "example": "ethereum"
          },
          {
            "name": "address",
            "in": "path",
            "description": "The contract address",
            "required": true,
            "schema": {
              "type": "string"
            },
            "example": 1.074999140385143e+48
          },
          {
            "name": "identifier",
            "in": "path",
            "description": "The NFT token id",
            "required": true,
            "schema": {
              "type": "string"
            },
            "example": 1
          },
          {
            "name": "ignoreCachedItemUrls",
            "in": "query",
            "description": "Whether to bypass cached SeaDN URLs",
            "required": false,
            "schema": {
              "type": "boolean"
            },
            "example": true
          }
        ],
        "responses": {
          "200": {
            "description": "OK",
            "content": {
              "*/*": {
                "schema": {
                  "$ref": "#/components/schemas/ValidateMetadataResponse"
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
            "$ref": "#/components/responses/NotFound"
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
      "MetadataIngestionError": {
        "type": "object",
        "properties": {
          "errorType": {
            "type": "string"
          },
          "message": {
            "type": "string"
          },
          "url": {
            "type": "string"
          },
          "statusCode": {
            "type": "integer",
            "format": "int32"
          }
        },
        "required": [
          "errorType",
          "message"
        ]
      },
      "ValidateMetadataAssetIdentifier": {
        "type": "object",
        "properties": {
          "chain": {
            "type": "string"
          },
          "contractAddress": {
            "type": "string"
          },
          "tokenId": {
            "type": "string"
          }
        },
        "required": [
          "chain",
          "contractAddress",
          "tokenId"
        ]
      },
      "ValidateMetadataAttribute": {
        "type": "object",
        "properties": {
          "traitType": {
            "type": "string"
          },
          "value": {
            "type": "string"
          },
          "displayType": {
            "type": "string"
          }
        },
        "required": [
          "traitType",
          "value"
        ]
      },
      "ValidateMetadataDetails": {
        "type": "object",
        "properties": {
          "name": {
            "type": "string"
          },
          "description": {
            "type": "string"
          },
          "originalImageUrl": {
            "type": "string"
          },
          "processedImageUrl": {
            "type": "string"
          },
          "originalAnimationUrl": {
            "type": "string"
          },
          "processedAnimationUrl": {
            "type": "string"
          },
          "externalUrl": {
            "type": "string"
          },
          "backgroundColor": {
            "type": "string"
          },
          "attributes": {
            "type": "array",
            "items": {
              "$ref": "#/components/schemas/ValidateMetadataAttribute"
            }
          }
        },
        "required": [
          "attributes"
        ]
      },
      "ValidateMetadataResponse": {
        "type": "object",
        "properties": {
          "assetIdentifier": {
            "$ref": "#/components/schemas/ValidateMetadataAssetIdentifier"
          },
          "tokenUri": {
            "type": "string"
          },
          "metadata": {
            "$ref": "#/components/schemas/ValidateMetadataDetails"
          },
          "error": {
            "$ref": "#/components/schemas/MetadataIngestionError"
          }
        },
        "required": [
          "assetIdentifier"
        ]
      }
    },
    "responses": {
      "BadRequest": {
        "description": "For error reasons, review the response data."
      },
      "NotFound": {
        "description": "Resource not found"
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