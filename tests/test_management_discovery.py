"""Generic filename discovery and executable management UI regressions."""

import json
import shutil
import subprocess

import pytest

from blobforge.server.database import Database
from blobforge.server.management_ui import JS


@pytest.mark.skipif(shutil.which("node") is None, reason="Node required")
def test_recipe_catalog_and_filter_show_distinct_versions():
    declarations = "\n".join(line for line in JS.splitlines() if line.startswith((
        "const recipeName=", "async function loadRecipes(")))
    script = """
const assert=require('node:assert/strict');
const state={},elements={'#job-recipe':{value:'new'},'#recipes-body':{}};
const $=s=>elements[s],$$=()=>[],esc=String,shortDigest=String,toast=e=>{throw Error(e)};
const api=async()=>({recipes:['1.2.0','1.4.0'].map((v,i)=>({recipe_digest:i?'new':'old',display_name:'mistral-ocr-wiki',backend:'mistral',media_types:['application/pdf'],enabled:true,recipe:{lifecycle:{recipe_version:v}}}))});
""" + declarations + """
(async()=>{await loadRecipes();for(const selector of ['#job-recipe','#recipes-body']){
assert.match(elements[selector].innerHTML,/recipe 1.2.0/);
assert.match(elements[selector].innerHTML,/recipe 1.4.0/);
}assert.equal(elements['#job-recipe'].value,'new');})().catch(e=>{console.error(e);process.exitCode=1});
"""
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_search_decodes_paths_casefolds_and_matches_filename_words(tmp_path):
    db = Database(tmp_path / "db.sqlite", lease_seconds=60, max_retries=3)
    db.enqueue("a" * 64, {"original_name": "ÜBER_Rules-Second.Edition.PDF",
                          "paths": ["Bücher/Straße.pdf"], "tags": ["Fantasy"]})
    db.enqueue("b" * 64, {"original_name": "Other.pdf"})
    for query in ["über rules", "SECOND edition.pdf", "bücher", "STRASSE", "fantasy", "a" * 64]:
        result = db.list_jobs(search=query)
        assert result["total"] == 1, query
        assert result["jobs"][0]["hash"] == "a" * 64
    assert db.list_jobs(search="%")['total'] == 0
    assert db.list_jobs(search="über", status="done")['total'] == 0
    assert db.list_jobs(search="über", offset=1)['jobs'] == []


@pytest.mark.skipif(shutil.which("node") is None, reason="Node is required for browser JS regression")
def test_ui_versions_and_search_response_order():
    declarations = "\n".join(line for line in JS.splitlines() if line.startswith((
        "const recipeName=", "function quotaBlockerText(", "function fxStatusText(", "let jobsRequest=", "async function loadJobs(")))
    script = """
const assert=require('node:assert/strict');
const state={recipes:[{recipe_digest:'new',backend:'mistral',recipe:{lifecycle:{recipe_version:'1.4.0',postprocessing:{profile:'wiki-v4'}}}}],offset:0,limit:50};
let params='old';const pending=[];const elements={};
const $=s=>elements[s]??=( {} ),$$=()=>[],jobRow=()=>'',toast=e=>{throw Error(e)};
const when=String,saveNavigation=()=>{},jobParams=()=>params,api=()=>new Promise(resolve=>pending.push(resolve));
""" + declarations + """
assert.match(recipeName('new'),/recipe 1.4.0.*wiki-v4/);
assert.match(recipeName('unknown'),/version unavailable/);
assert.match(quotaBlockerText(JSON.stringify({kind:'quota',exceeded:[{dimension:'billed',used:12737966,requested:1279643,limit:12750000}]}),'EUR'),/1.267609 EUR/);
assert.match(quotaBlockerText(JSON.stringify({kind:'quota',exceeded:[{dimension:'provider_snapshot'}]}),'EUR'),/cannot bypass/);
assert.match(fxStatusText({last_success:1,error:'TimeoutError',warnings:[]}),/Warning: TimeoutError.*never blocks jobs/);
assert.match(fxStatusText({last_success:1,warnings:[{account_key:'test',source_currency:'USD',message:'bundled fallback'}]}),/bundled fallback/);
(async()=>{
 const old=loadJobs();params='new';const current=loadJobs();
 pending[1]({jobs:[{hash:'new'}],total:1});await current;
 pending[0]({jobs:[{hash:'old'}],total:99});await old;
 assert.equal(state.total,1);assert.equal(state.jobs[0].hash,'new');
})().catch(e=>{console.error(e);process.exitCode=1});
"""
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
