"""Execute the browser's navigation/polling logic rather than matching prose."""
import shutil
import subprocess

import pytest

from blobforge.server.management_navigation import JS
from blobforge.server.management_ui import JS as UI_JS


@pytest.mark.skipif(shutil.which("node") is None, reason="Node needed for browser behavior")
def test_reload_routes_filters_details_and_nonoverlapping_refresh():
    declarations = JS.split("\nwindow.addEventListener")[0]
    script = r"""
const assert=require('node:assert/strict');
const elements={};const $=s=>elements[s]??={value:'',open:false,close(){this.open=false},addEventListener(name,cb){this.onClose=cb}};
const state={view:'dashboard',offset:0};let detailRequest=0,selected=null,editing=false,selection='',otherDialog=false;
const location={hash:''};const history={pushState(a,b,url){location.hash=url},replaceState(a,b,url){location.hash=url}};
const document={hidden:false,querySelector(){return otherDialog},activeElement:{matches(selector){return editing&&selector==='input,textarea,select'}}};
const window={getSelection(){return {toString(){return selection}}}};
const show=v=>state.view=v,loadRecipes=async()=>{},openJob=async key=>{selected=key;state.detailKey=key};
let calls=0,finish;const loadJobs=async()=>{calls++;await new Promise(r=>finish=r)};
const loadOverview=async()=>{},loadWorkers=async()=>{},loadQuotas=async()=>{},loadQuotaActions=async()=>{},loadTokens=async()=>{};
""" + declarations + r"""
(async()=>{
 location.hash='#view=jobs&search=%C3%9Cber+Rules&status=todo&priority=2_high&recipe_digest=recipe1&offset=50&job=abc';
 await restoreNavigation();
 assert.equal(state.view,'jobs');assert.equal(state.offset,50);assert.equal(selected,'abc');
 assert.equal($('#job-search').value,'Über Rules');assert.equal($('#job-recipe').value,'recipe1');
 assert.equal($('#job-status').value,'todo');assert.equal($('#job-priority').value,'2_high');
 saveNavigation();assert.match(location.hash,/job=abc/);
 location.hash='#view=invalid&offset=-5';await restoreNavigation();assert.equal(state.view,'dashboard');assert.equal(state.offset,0);
 $('#detail-dialog').open=true;closeDetailsForNavigation();state.detailKey='new';
 $('#detail-dialog').onClose();assert.equal(state.detailKey,'new');
 $('#detail-dialog').onClose();assert.equal(state.detailKey,null);
 state.view='jobs';state.detailKey=null;
 document.hidden=true;await refreshVisible();assert.equal(calls,0);document.hidden=false;
 editing=true;await refreshVisible();assert.equal(calls,0);editing=false;
 otherDialog=true;await refreshVisible();assert.equal(calls,0);otherDialog=false;
 selection='traceback';await refreshVisible();assert.equal(calls,0);selection='';
 const first=refreshVisible();await refreshVisible();assert.equal(calls,1);finish();await first;
 const second=refreshVisible();assert.equal(calls,2);finish();await second;
})().catch(e=>{console.error(e);process.exitCode=1});
"""
    result = subprocess.run(["node", "-e", script], text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.skipif(shutil.which("node") is None, reason="Node needed for browser behavior")
def test_closed_job_does_not_reopen_when_detail_request_finishes():
    declarations = "\n".join(line for line in UI_JS.splitlines() if line.startswith((
        "let detailRequest=", "async function openJobV7(")))
    script = r"""
const assert=require('node:assert/strict');
const state={recipes:[]};let complete,opened=false;
const api=()=>new Promise(r=>complete=r),saveNavigation=()=>{},loadRecipes=async()=>{};
const $=()=>({showModal(){opened=true}}),toast=e=>{throw Error(e)};
""" + declarations + r"""
(async()=>{const pending=openJobV7('old');state.detailKey=null;detailRequest++;
complete({job:{hash:'old'}});await pending;assert.equal(opened,false);
})().catch(e=>{console.error(e);process.exitCode=1});
"""
    result = subprocess.run(["node", "-e", script], text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
