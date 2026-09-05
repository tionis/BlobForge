"""Browser navigation and bounded, non-mutating management refresh."""

JS = r"""
const views=['dashboard','jobs','workers','recipes','quotas','access'];
const jobFilters=[['search','#job-search'],['status','#job-status'],['priority','#job-priority'],['recipe_digest','#job-recipe']];
let restoringNavigation=false,navigationRequest=0,refreshBusy=false,ignoredDetailCloses=0;
function closeDetailsForNavigation(){state.detailKey=null;detailRequest++;const dialog=$('#detail-dialog');if(dialog.open){ignoredDetailCloses++;dialog.close()}}
function navigationParams(){const p=new URLSearchParams({view:state.view});if(state.view==='jobs'){for(const [key,selector] of jobFilters){const value=$(selector).value;if(value)p.set(key,value)}if(state.offset)p.set('offset',state.offset);if(state.detailKey)p.set('job',state.detailKey)}return p}
function saveNavigation(push=false){if(restoringNavigation)return;const hash='#'+navigationParams();if(location.hash!==hash)history[push?'pushState':'replaceState'](null,'',hash)}
async function restoreNavigation(){const request=++navigationRequest;restoringNavigation=true;try{const p=new URLSearchParams(location.hash.slice(1));const view=views.includes(p.get('view'))?p.get('view'):'dashboard';closeDetailsForNavigation();if(view==='jobs'&&p.get('recipe_digest'))await loadRecipes();if(request!==navigationRequest)return;for(const [key,selector] of jobFilters)$(selector).value=p.get(key)||'';const offset=Number(p.get('offset')||0);state.offset=Number.isSafeInteger(offset)&&offset>=0?offset:0;show(view,false);const key=p.get('job');if(view==='jobs'&&key&&/^[A-Za-z0-9._:-]{1,256}$/.test(key))await openJob(key)}finally{if(request===navigationRequest){restoringNavigation=false;saveNavigation()}}}
function refreshPaused(){return document.hidden||!!document.querySelector('dialog[open]:not(#detail-dialog)')||(!!document.activeElement?.matches('input,textarea,select')&&!document.activeElement.matches('#job-search,#job-status,#job-priority,#job-recipe'))||!!window.getSelection()?.toString()}
async function refreshVisible(){if(refreshBusy||restoringNavigation||refreshPaused())return;refreshBusy=true;try{if(state.view==='jobs'){await loadJobs(true);if(state.detailKey&&$('#detail-dialog').open&&!refreshPaused())await openJob(state.detailKey,true)}else if(state.view==='dashboard')await loadOverview();else if(state.view==='workers')await loadWorkers();else if(state.view==='recipes')await loadRecipes();else if(state.view==='quotas'){await loadQuotas();await loadQuotaActions()}else if(state.view==='access')await loadTokens()}finally{refreshBusy=false}}
$('#detail-dialog').addEventListener('close',()=>{if(ignoredDetailCloses){ignoredDetailCloses--;return}if($('#detail-dialog').open)return;state.detailKey=null;detailRequest++;saveNavigation()});
window.addEventListener('popstate',restoreNavigation);
window.addEventListener('hashchange',restoreNavigation);
document.addEventListener('visibilitychange',()=>{if(!document.hidden)refreshVisible()});
setInterval(refreshVisible,10000);
restoreNavigation();
"""
