from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from queries import get_issues_summary, get_issue_details, get_entries_for_issue, get_user
from sqlalchemy.engine import create_engine
import os
from dotenv import load_dotenv
import boto3
from aws import get_object



load_dotenv()

app = FastAPI()
templates = Jinja2Templates(directory="templates")

denylist_engine = create_engine(os.getenv("DENYLIST_DB_CONNECTION_STRING"))

s3 = boto3.resource("s3")


@app.get("/", response_class=HTMLResponse)
async def summary(request: Request):
    issues = get_issues_summary(denylist_engine)
    return templates.TemplateResponse("index.html", {"request": request, "issues": issues})


@app.get("/issue/{number}", response_class=HTMLResponse)
async def issue(request: Request, number):
    details = get_issue_details(denylist_engine, number, with_body=False)
    entries = get_entries_for_issue(denylist_engine, number)
    return templates.TemplateResponse("issue.html", {"request": request, "details": details, "entries": entries})


@app.get("/user/{user_id}", response_class=HTMLResponse)
async def user(request: Request, user_id):
    details = get_user(denylist_engine, user_id)
    return templates.TemplateResponse("user.html", {"request": request, "details": details})


@app.get("/issue/{number}/{address}/data", response_class=JSONResponse)
async def data(request: Request, number, address):
    distance_vs_rssi = get_object(s3, os.getenv("S3_BUCKET"), key=f"issues/{number}/entries/{address}/distance_vs_rssi")
    witnessed_makers = get_object(s3, os.getenv("S3_BUCKET"), key=f"issues/{number}/entries/{address}/witnessed_makers")
    hotspot_details = get_object(s3, os.getenv("S3_BUCKET"), key=f"issues/{number}/entries/{address}/hotspot_details")
    witness_graph = get_object(s3, os.getenv("S3_BUCKET"), key=f"issues/{number}/entries/{address}/witness_graph")
    return JSONResponse({"distance_vs_rssi": distance_vs_rssi,
                         "witnessed_makers": witnessed_makers,
                         "hotspot_details": hotspot_details,
                         "witness_graph": witness_graph})


@app.get("/issue/{number}/{address}", response_class=HTMLResponse)
async def entry(request: Request, number, address):
    return templates.TemplateResponse("entry.html", {"request": request, "number": number, "address": address})
