#!/usr/bin/env bash
set -euo pipefail
umask 077

mode=$1
tool=$2
root=$3
public=$4
private=$5
mkdir -p "$public" "$private"
candidate="$root/deploy/j7/controller"
gate="$public/admission.json"
state="$public/campaign.json"
armed=0
done_count=0
monitor_pid=
active_drill=
status=invalid
reason=admission
elapsed() {
    local now
    now=$(awk '{print int($1)}' /proc/uptime)
    printf '%s\n' "$(( (now - J7_JOB_START + 59) / 60 ))"
}
if [[ "$mode" == qualification ]]; then
    drills=(q1)
elif [[ "$mode" == measurement ]]; then
    drills=(r1 r2 r3)
else
    exit 2
fi

finish() {
    local original=$? cleanup=false id final_code=0
    trap - EXIT
    set +e
    if [[ -n "$monitor_pid" ]]; then
        kill "$monitor_pid" 2>/dev/null
        wait "$monitor_pid" 2>/dev/null
    fi
    if [[ -n "$active_drill" && ! -e "$public/$active_drill/evidence" ]]; then
        local partial="$private/$active_drill" keep="$public/$active_drill" input_digest keep_digest
        mkdir -p "$keep"
        if [[ -f "$partial/index.json" ]]; then
            input_digest=$(sha256sum "$partial/index.json" | cut -d ' ' -f 1)
        else
            input_digest=$("$tool" index-evidence --dir "$partial" --revision "$GITHUB_SHA" \
                --campaign "$J7_CAMPAIGN" --drill "$active_drill" --run "$GITHUB_RUN_ID" \
                --attempt "$GITHUB_RUN_ATTEMPT" | jq -er .evidence_index_sha256)
        fi
        if [[ -n "$input_digest" ]] && keep_digest=$("$tool" publish-evidence --dir "$partial" \
            --index-sha256 "$input_digest" --destination "$keep/evidence" | jq -er .evidence_index_sha256); then
            printf '%s\n' "$keep_digest" >"$keep/index.sha256"
        fi
    fi
    if (( armed )); then
        local cleanup_started=$SECONDS containers volumes
        if timeout --kill-after=10s 20m "$candidate" cleanup "$private" >"$private/cleanup.stdout" 2>&1 &&
            jq -e '.verified == true and .residuals == []' "$private/cleanup.json" >/dev/null &&
            containers=$(docker --host=unix:///var/run/docker.sock container ls -aq --filter "label=j7.restore.run=$GITHUB_RUN_ID") &&
            volumes=$(docker --host=unix:///var/run/docker.sock volume ls -q --filter "label=j7.restore.run=$GITHUB_RUN_ID") &&
            [[ -z "$containers" && -z "$volumes" ]]; then
            cleanup=true
        elif [[ "$status" != failed ]]; then
            status=invalid
            reason=cleanup
        fi
        local engine_residuals
        engine_residuals=$(printf '%s\n%s\n' "${containers:-}" "${volumes:-}" | jq -Rsc 'split("\n") | map(select(length>0))')
        if [[ -s "$private/cleanup.json" ]]; then
            jq --argjson verified "$cleanup" --argjson docker_residuals "$engine_residuals" \
                '{verified:$verified,residuals:((.residuals // []) + $docker_residuals)}' \
                "$private/cleanup.json" >"$public/cleanup.json"
        fi
        if [[ "$status" == passed && -s "$private/stage-times.json" ]]; then
            local cleanup_minutes=$(( (SECONDS - cleanup_started + 59) / 60 ))
            jq --argjson minutes "$cleanup_minutes" '.final=[.final,$minutes]|.final|=max' \
                "$private/stage-times.json" >"$private/observed-stage-times.json" &&
                mv -- "$private/observed-stage-times.json" "$private/stage-times.json"
        fi
    fi
    # Emit exact dispositions for unstarted identities; preserve a recorded
    # failure through subsequent retention or teardown problems.
    local roster=()
    for id in "${drills[@]}"; do
        if [[ -f "$public/$id/disposition.json" ]]; then
            roster+=("$public/$id/disposition.json")
        elif [[ "$active_drill" == "$id" ]]; then
            local stop_status=invalid
            [[ "$status" != failed ]] || stop_status=failed
            mkdir -p "$public/$id"
            jq -n --arg id "$id" --arg reason "$reason" --arg status "$stop_status" \
                '{drill:$id,status:$status,reason:$reason}' >"$public/$id/disposition.json"
            roster+=("$public/$id/disposition.json")
        else
            mkdir -p "$public/$id"
            jq -n --arg id "$id" --arg reason "$reason" \
                '{drill:$id,status:"unperformed",reason:$reason}' >"$public/$id/disposition.json"
            roster+=("$public/$id/disposition.json")
        fi
    done
    if [[ "$status" == passed && ( "$cleanup" != true || "$original" != 0 ) ]]; then
        status=invalid
    fi
    jq -s --arg status "$status" --arg reason "$reason" --arg mode "$mode" \
        --arg revision "$GITHUB_SHA" --argjson cleanup "$cleanup" \
        '{schema:"j7.campaign.v1",mode:$mode,revision:$revision,status:$status,reason:$reason,cleanup_verified:$cleanup,drills:.}' \
        "${roster[@]}" >"$state" || final_code=1
    if [[ "$mode" == qualification && "$status" == passed ]]; then
        jq -n --arg revision "$GITHUB_SHA" --arg candidate "$(jq -er .candidate_sha256 "$gate")" \
            --arg index "$(cat "$public/q1/index.sha256")" --argjson run "$GITHUB_RUN_ID" \
            --argjson attempt "$GITHUB_RUN_ATTEMPT" --slurpfile stage "$private/stage-times.json" \
            --slurpfile controls "$public/qualification-controls.json" \
            '{schema:"j7.qualification.v1",revision:$revision,candidate_sha256:$candidate,run_id:$run,run_attempt:$attempt,
              status:"passed",drill_status:"passed",drill_index_sha256:$index,controls:$controls[0].codes,
              replay_passed:true,cleanup_verified:true,stage_minutes:$stage[0]}' >"$public/qualification.json" || final_code=1
    fi
    if [[ "$status" != passed || "$original" != 0 ]]; then final_code=1; fi
    exit "$final_code"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

test "$GITHUB_RUN_ATTEMPT" = 1
test "$(git -C "$root" rev-parse HEAD)" = "$GITHUB_SHA"
gate_args=()
if [[ "$mode" == measurement ]]; then
    gate_args=(--qualification "$J7_QUALIFICATION" --qualification-run "$J7_QUALIFICATION_RUN" --revision "$GITHUB_SHA")
fi
"$tool" hosted-gate --root "$root" --mode "$mode" --elapsed-minutes "$(elapsed)" "${gate_args[@]}" \
    >"$gate" 2>"$public/admission-error.txt"
# Only a reviewed, tracked controller may start resources on a hosted job.
git -C "$root" ls-files --error-unmatch deploy/j7/candidate.json deploy/j7/controller >/dev/null
test -x "$candidate"
armed=1
reason=bootstrap
bootstrap_started=$SECONDS
timeout --kill-after=10s 35m "$candidate" bootstrap "$private" >"$private/bootstrap.log" 2>&1
bootstrap_time=$(( (SECONDS - bootstrap_started + 59) / 60 ))
observed_drill=0
observed_between=0
observed_controls=0
observed_final=0

if [[ "$mode" == measurement ]]; then
    qualified="$J7_QUALIFICATION"
    observed_drill=$(jq -er .stage_minutes.drill "$qualified")
    observed_between=$(jq -er .stage_minutes.between "$qualified")
    observed_final=$(jq -er .stage_minutes.final "$qualified")
fi

save_times() {
    jq -n --argjson boot "$bootstrap_time" --argjson drill "$observed_drill" \
        --argjson between "$observed_between" --argjson controls "$observed_controls" --argjson final "$observed_final" \
        '{bootstrap:$boot,drill:$drill,between:$between,controls:$controls,final:$final}' >"$private/stage-times.json"
}
swap_used() {
    awk '/^SwapTotal:/ {total=$2} /^SwapFree:/ {free=$2} END {print total-free}' /proc/meminfo
}
scratch_free_kib() {
    local a b
    a=$(df -Pk -- "$private" | awk 'END {print $4}')
    b=$(df -Pk -- /var/lib/docker | awk 'END {print $4}')
    if (( a < b )); then printf '%s\n' "$a"; else printf '%s\n' "$b"; fi
}
sample_loop() {
    local free memory swap
    while true; do
        free=$(scratch_free_kib)
        memory=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
        swap=$(swap_used)
        printf '%s\t%s\t%s\t%s\n' "$(date -u +%FT%TZ)" "$free" "$memory" "$swap" >>"$public/host-capacity.tsv"
        if (( free < 4*1024*1024 || memory < 2*1024*1024 || swap > baseline_swap )); then
            printf '%s\n' "runner capacity violated" >"$private/resource-stop"
            return
        fi
        sleep 5
    done
}
baseline_swap=$(swap_used)
sample_loop &
monitor_pid=$!

for id in "${drills[@]}"; do
    reason=forecast
    save_times
    "$tool" hosted-gate --mode "$mode" --forecast-only --bootstrapped --stage-times "$private/stage-times.json" \
        --elapsed-minutes "$(elapsed)" --completed-drills "$done_count" >"$public/forecast-$id.json"
    reason=capacity
    (( $(nproc) >= 4 ))
    (( $(awk '/^MemTotal:/ {print $2}' /proc/meminfo) >= 12*1024*1024 ))
    (( $(scratch_free_kib) >= 8*1024*1024 ))
    test ! -e "$private/resource-stop"
    kill -0 "$monitor_pid"
    reason=drill
    active_drill=$id
    raw="$private/$id"
    retained="$public/$id"
    mkdir -p "$raw" "$retained"
    started=$SECONDS
    result=0
    timeout --kill-after=10s 20m "$candidate" drill "$id" "$private" "$raw" "$tool" >"$raw/controller.log" 2>&1 &
    driver_pid=$!
    while kill -0 "$driver_pid" 2>/dev/null; do
        if [[ -f "$private/resource-stop" ]] || ! kill -0 "$monitor_pid" 2>/dev/null; then
            kill -TERM "$driver_pid" 2>/dev/null || true
            reason=capacity
            break
        fi
        sleep 1
    done
    wait "$driver_pid" || result=$?
    # Driver logs, private settings, raw credentials, backup and WAL never
    # enter the upload path. The independent index is the allowlist.
    index_args=(--revision "$GITHUB_SHA" --campaign "$J7_CAMPAIGN" --drill "$id" --run "$GITHUB_RUN_ID" --attempt "$GITHUB_RUN_ATTEMPT")
    verified=0
    bundle="$raw"
    if index=$("$tool" index-evidence --dir "$raw" "${index_args[@]}" | jq -er .evidence_index_sha256); then
        live_result=0
        "$tool" verify-drill --dir "$raw" --index-sha256 "$index" >"$private/$id-verdict.json" 2>"$private/$id-verdict.log" || live_result=$?
        if [[ -s "$private/$id-verdict.json" ]]; then
            if [[ "$(jq -r .status "$private/$id-verdict.json")" == failed ]]; then status=failed; fi
            # The first index is the input seal; the second adds the verdict.
            mv -- "$raw/index.json" "$private/$id-input-index.json"
            cp -- "$private/$id-verdict.json" "$raw/verdict.json"
            index=$("$tool" index-evidence --dir "$raw" "${index_args[@]}" | jq -er .evidence_index_sha256)
        fi
        if publish=$("$tool" publish-evidence --dir "$raw" --index-sha256 "$index" --destination "$retained/evidence" |
            jq -er .evidence_index_sha256); then
            printf '%s\n' "$publish" >"$retained/index.sha256"
            if (( live_result == 0 )) &&
                "$tool" verify-drill --replay --dir "$retained/evidence" --index-sha256 "$publish" >"$retained/replay.json"; then
                verified=1
            fi
        fi
    fi
    duration=$(( (SECONDS - started + 59) / 60 ))
    (( duration <= observed_drill )) || observed_drill=$duration
    outcome=invalid
    if [[ -s "$bundle/verdict.json" ]] && [[ "$(jq -r .status "$bundle/verdict.json")" == failed ]]; then
        outcome=failed
    elif (( verified && result == 0 && duration <= 20 )) && [[ ! -e "$private/resource-stop" ]]; then
        outcome=passed
    fi
    jq -n --arg id "$id" --arg status "$outcome" --argjson duration "$duration" \
        '{drill:$id,status:$status,minutes:$duration}' >"$retained/disposition.json"
    active_drill=
    if [[ "$outcome" != passed ]]; then
        status=$outcome
        exit 1
    fi
    done_count=$((done_count + 1))
    if (( done_count < ${#drills[@]} )); then
        reason=between_drills
        started=$SECONDS
        timeout --kill-after=10s 5m "$candidate" between "$id" "$private" "$raw" >"$private/between-$id.log" 2>&1
        duration=$(( (SECONDS - started + 59) / 60 ))
        (( duration <= observed_between )) || observed_between=$duration
    fi
done

if [[ "$mode" == qualification ]]; then
    reason=qualification_controls
    started=$SECONDS
    tail="$private/q1/source-tail.private.json"
    tail_sha=$(sha256sum "$tail" | cut -d ' ' -f 1)
    timeout --kill-after=10s 15m "$tool" verify-controls --dir "$private/q1" --index-sha256 "$index" \
        --archive "$private/q1/media/archive" --source-tail "$tail" --source-tail-sha256 "$tail_sha" --scratch "$private" \
        >"$private/qualification-controls.json" 2>"$private/controls.log"
    jq -e '.healthy_sibling == true and
          .codes == ["PRE_CUT_MARKER_MISSING","POST_CUT_MARKER_PRESENT","WAL_ARCHIVE_GAP","CONSUMER_GENERATION_MISMATCH"]' \
        "$private/qualification-controls.json" > /dev/null
    jq '{healthy_sibling,codes}' "$private/qualification-controls.json" >"$public/qualification-controls.json"
    observed_controls=$(( (SECONDS - started + 59) / 60 ))
fi
save_times
status=passed
reason=complete
