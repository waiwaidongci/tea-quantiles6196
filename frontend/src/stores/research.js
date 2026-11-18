import {studies,runs} from '../api/research.js';
import {largeRuns} from '../api/large.js';
export const store={studies:[],runs:[],largeRuns:[],studyId:0,runId:0,largeRunId:0,largeEcdfPage:0,rows:[]};
export async function load(){[store.studies,store.runs]=await Promise.all([studies.list(),runs.list()]);if(!store.studyId)store.studyId=store.studies[0]?.id||0;}
export async function loadLarge(){store.largeRuns=await largeRuns.list();}
export async function loadRows(){store.rows=store.studyId?await studies.rows(store.studyId):[];}
