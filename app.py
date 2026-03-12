import uvicorn
from fastapi import FastAPI

from futurae_assignment.api.routers.events import router as events_router
from futurae_assignment.api.routers.metrics import router as metrics_router

app = FastAPI(title="Futurae Assignment API")

app.include_router(events_router)
app.include_router(metrics_router)

if __name__ == "__main__":
    uvicorn.run(app)
