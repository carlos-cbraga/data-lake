# System API

from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import JSONResponse
from google.cloud import storage
import os
from typing import Optional
import time

app = FastAPI(title="Data Ingest")

# Initialize GCS client
storage_client = storage.Client(project="data-lake-seminar")
BUCKET_NAME = "data-lake"

#endpoints: 
@app.get("/")
async def root():
    return {"message": "Data Lake API"}

@app.post("/exame/imagem")
async def upload_image(
    file: UploadFile = File(...),
    patient_id: str = Form(...),
    img_id: str = Form(...),
    dia: int = Form(...), 
    mes: int = Form(...),
    ano: int = Form(...),
):
    try:
        # Validate parameters
        if not patient_id or not img_id or not dia or not mes or not ano:
            raise HTTPException(status_code=400, detail="Parameters missing")
        
        # Construct GCS path
        gcs_path = f"{ano}-{mes}-{dia}/exame/imagens/{patient_id}-{img_id}"
        
        # Get bucket
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        
        # Custom metadata
        custom_metadata = {
            "patient_id": patient_id,
            "img_id": img_id,
            "fonte": "hospital_api",
        }
        blob.metadata = custom_metadata

        # Upload file
        contents = await file.read()
        blob.upload_from_string(contents, content_type=file.content_type)
        
        return JSONResponse(
            status_code=201,
            content={
                "message": "Uploaded successfully",
                "gcs_path": f"gs://{BUCKET_NAME}/{gcs_path}",
                "size_bytes": len(contents)
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.post("/exame/resultado")
async def upload_resultado(
    file: UploadFile = File(...),
    patient_id: str = Form(...),
    res_id: str = Form(...),
    dia: int = Form(...), 
    mes: int = Form(...),
    ano: int = Form(...),
):
    try:
        # Validate 
        if not patient_id or not res_id or not dia or not mes or not ano:
            raise HTTPException(status_code=400, detail="parameters missing")
        
        # Construct GCS path
        gcs_path = f"{ano}-{mes}-{dia}/exame/resultados/{patient_id}-{res_id}"
        
        # Get bucket
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        
        # Custom metadata
        custom_metadata = {
            "patient_id": patient_id,
            "res_id": res_id,
            "fonte": "hospital_api",
        }
        blob.metadata = custom_metadata

        # Upload file
        contents = await file.read()
        blob.upload_from_string(contents, content_type=file.content_type)
        
        return JSONResponse(
            status_code=201,
            content={
                "message": "Uploaded successfully",
                "gcs_path": f"gs://{BUCKET_NAME}/{gcs_path}",
                "size_bytes": len(contents)
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.post("/financeiro/NF")
async def upload_NF(
    file: UploadFile = File(...),
    dia: int = Form(...), 
    mes: int = Form(...),
    ano: int = Form(...),
    NF: str = Form(...)
):
    try:
        # Validate 
        if not dia or not mes or not ano or not NF:
            raise HTTPException(status_code=400, detail="parameters missing")
        
        # Construct GCS path
        gcs_path = f"{ano}-{mes}-{dia}/financeiro/NF/{NF}"
        
        # Get bucket
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        
        # Custom metadata
        custom_metadata = {
            "NF": NF,
            "fonte": "hospital_api",
        }
        blob.metadata = custom_metadata

        # Upload file
        contents = await file.read()
        blob.upload_from_string(contents, content_type=file.content_type)
        
        return JSONResponse(
            status_code=201,
            content={
                "message": "Uploaded successfully",
                "NF": NF,
                "gcs_path": f"gs://{BUCKET_NAME}/{gcs_path}",
                "size_bytes": len(contents)
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.post("/mkt/relatorio")
async def upload_relatorio_mkt(
    file: UploadFile = File(...),
    dia: int = Form(...), 
    mes: int = Form(...),
    ano: int = Form(...),
    campanha_id: str = Form(...)
):
    try:
        # Validate 
        if not dia or not mes or not ano or not campanha_id:
            raise HTTPException(status_code=400, detail="parameters missing")
        
        # Construct GCS path
        gcs_path = f"{ano}-{mes}-{dia}/mkt/relatorio/{campanha_id}"
        
        # Get bucket
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        
        # Custom metadata
        custom_metadata = {
            "campanha": campanha_id,
            "fonte": "hospital_api",
        }
        blob.metadata = custom_metadata

        # Upload file
        contents = await file.read()
        blob.upload_from_string(contents, content_type=file.content_type)
        
        return JSONResponse(
            status_code=201,
            content={
                "message": "Uploaded successfully",
                "campanha_id": campanha_id,
                "gcs_path": f"gs://{BUCKET_NAME}/{gcs_path}",
                "size_bytes": len(contents)
            }
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint for Cloud Run"""
    return {"status": "healthy"}
