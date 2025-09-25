import os
import httpx

async def trigger_bulk_load(s3_input_uri: str, mode: str = "NEW") -> dict:
    """
    Async Neptune Bulk Loader trigger
    """
    neptune_endpoint = os.getenv("NEPTUNE_ENDPOINT")
    iam_role_arn = os.getenv("NEPTUNE_IAM_ROLE_ARN")
    region = os.getenv("AWS_REGION")
    loader_url = f"https://{neptune_endpoint}:8182/loader"

    payload = {
        "source": s3_input_uri,
        "format": "csv",
        "iamRoleArn": iam_role_arn,
        "region": region,
        "mode": mode,
        "failOnError": True,
        "parallelism": "MEDIUM",
        "updateSingleCardinalityProperties": True
    }

    async with httpx.AsyncClient(verify=False) as client:
        response = await client.post(loader_url, json=payload)

    if response.status_code not in [200, 201]:
        raise Exception(f"Bulk load failed: {response.text}")

    return response.json()
