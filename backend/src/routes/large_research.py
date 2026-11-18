from fastapi import APIRouter,Depends
from fastapi.responses import Response
from src.config.database import get_db
from src.middlewares.auth import current_user,writer,admin
from src.schemas.large_run import LargeRunInput,LargeBatchInput
from src.schemas.review import ReviewInput
from src.services import large_runs
router=APIRouter(prefix='/large-runs',tags=['large-runs'])

@router.get('')
def list_runs(db=Depends(get_db),user=Depends(current_user)):
    return large_runs.list_runs(db)

@router.post('')
def create(data:LargeRunInput,db=Depends(get_db),user=Depends(writer)):
    return large_runs.create(db,data,user)

@router.get('/{run_id}')
def get_run(run_id:int,db=Depends(get_db),user=Depends(current_user)):
    return large_runs.get(db,run_id)

@router.post('/{run_id}/batches')
def add_batch(run_id:int,data:LargeBatchInput,db=Depends(get_db),user=Depends(writer)):
    return large_runs.add_batch(db,run_id,data,user)

@router.post('/{run_id}/freeze')
def freeze(run_id:int,db=Depends(get_db),user=Depends(writer)):
    return large_runs.freeze(db,run_id,user)

@router.get('/{run_id}/ecdf')
def ecdf(run_id:int,page:int=0,db=Depends(get_db),user=Depends(current_user)):
    return large_runs.ecdf(db,run_id,page)

@router.post('/{run_id}/review')
def review(run_id:int,data:ReviewInput,db=Depends(get_db),user=Depends(admin)):
    return large_runs.approve(db,run_id,data.note,user)

@router.get('/{run_id}/export')
def export(run_id:int,db=Depends(get_db),user=Depends(current_user)):
    return Response(content=large_runs.export_metrics(db,run_id,user).encode('utf-8'),
                    media_type='text/csv',
                    headers={'Content-Disposition':f'attachment; filename="large-research-{run_id}.csv"'})
