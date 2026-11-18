import {request,post} from './client.js';
export const largeRuns={
  list:()=>request('/large-runs'),
  create:x=>post('/large-runs',x),
  get:id=>request(`/large-runs/${id}`),
  addBatch:(id,x)=>post(`/large-runs/${id}/batches`,x),
  freeze:id=>post(`/large-runs/${id}/freeze`,{}),
  ecdf:(id,page)=>request(`/large-runs/${id}/ecdf?page=${page}`),
  review:(id,note)=>post(`/large-runs/${id}/review`,{note}),
  export:id=>request(`/large-runs/${id}/export')
};
