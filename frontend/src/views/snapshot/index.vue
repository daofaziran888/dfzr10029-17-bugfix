<template>
  <section class="page" data-module="snapshot">
    <header class="page-head">
      <div>
        <h2>数据快照</h2>
        <p class="page-desc">
          维护快照记录，围绕快照编号、数据集名称、快照时间、行数快照做登记、筛选与状态流转。
          快照完成状态与分片校验结果一起判定：失败分片不会进入可恢复快照。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记快照记录</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>

    <div class="filter-bar">
      <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <button class="btn" type="button" data-testid="query" @click="reload">查询</button>
    </div>

    <div class="timeline-wrap">
      <div v-for="item in timelineItems" :key="item.id" class="timeline-row">
        <span class="timeline-label">{{ item.name || item.label }}</span>
        <div class="timeline-track">
          <div
            class="timeline-bar"
            :class="`status-${item.status}`"
            :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }"
            :data-testid="`bar-${item.id}`"
            :title="`${item.start} ~ ${item.end}`"
            @click="selectItem(item)"
          >&nbsp;</div>
        </div>
        <span style="font-size:12px;color:var(--muted);margin-left:8px;">{{ item.start }} ~ {{ item.end }}</span>
        <span class="timeline-meta" data-testid="timeline-meta">{{ item.status }} · 版本 {{ item.version }}</span>
      </div>
      <p v-if="!timelineItems.length" class="empty-state">暂无作业记录</p>
    </div>

    <div v-if="selectedItem" class="timeline-log" data-testid="timeline-log">
      <strong>{{ selectedItem.name || selectedItem.label }}</strong>
      — 状态：{{ selectedItem.status }} · 版本：{{ selectedItem.version }}

      <div class="snapshot-boundary">
        <span class="boundary-item" :data-tone="reasonTone(selectedItem.reasonType)">
          校验结论：{{ selectedItem.reasonType || '通过' }} — {{ selectedItem.reason }}
        </span>
        <span class="boundary-item">
          当前可恢复版本：{{ selectedItem.recoverableVersion || '暂无' }}
        </span>
        <span class="boundary-item">
          失败分片：{{ (selectedItem.failedShards || []).length ? selectedItem.failedShards.join('、') : '无' }}
        </span>
      </div>

      <div style="margin-top:4px;white-space:pre-line;">{{ selectedItem.logs || '暂无日志' }}</div>

      <div v-if="detail" class="snapshot-detail" data-testid="snapshot-detail">
        <div class="detail-grid">
          <div><span class="detail-label">分片记录</span>{{ detail['分片校验汇总']?.['记录数'] ?? 0 }}</div>
          <div><span class="detail-label">通过</span>{{ detail['分片校验汇总']?.['通过'] ?? 0 }}</div>
          <div><span class="detail-label">无数据</span>{{ detail['分片校验汇总']?.['无数据'] ?? 0 }}</div>
          <div><span class="detail-label">失败</span>{{ detail['分片校验汇总']?.['失败'] ?? 0 }}</div>
          <div><span class="detail-label">缺失分片</span>{{ (detail['缺失分片'] || []).join('、') || '无' }}</div>
          <div><span class="detail-label">行数快照</span>{{ detail['行数快照'] ?? 0 }}</div>
        </div>
        <p class="detail-hint">
          恢复建议：{{ detail['恢复建议']?.方式 }}（{{ detail['恢复建议']?.说明 }}）
        </p>
        <div class="row-actions">
          <button class="btn" type="button" data-testid="open-shard" @click="openShardForm">提交分片校验</button>
          <button
            v-if="detail['可恢复']"
            class="btn primary"
            type="button"
            data-testid="recover"
            @click="recoverSelected()"
          >恢复当前版本</button>
          <button
            v-else-if="detail['当前可恢复版本']"
            class="btn primary"
            type="button"
            data-testid="recover-previous"
            @click="recoverSelected(detail['当前可恢复版本']['版本'])"
          >恢复上一份完整快照 {{ detail['当前可恢复版本']['版本'] }}</button>
          <button
            v-else
            class="btn"
            type="button"
            data-testid="recover"
            disabled
          >无可恢复版本</button>
          <button
            v-for="action in actions"
            :key="action"
            class="link"
            type="button"
            :data-testid="`action-${action}`"
            @click="runAction(action)"
          >{{ action }}</button>
        </div>
        <p v-if="actionMessage" class="action-message" data-testid="action-message">{{ actionMessage }}</p>
      </div>
    </div>

    <section v-if="selectedItem" class="shard-panel">
      <header class="shard-head">
        <h3>快照分片校验记录</h3>
        <div class="filter-bar" style="margin:0">
          <label>
            <span>版本范围</span>
            <select v-model="shardVersion" data-testid="shard-version" @change="reloadShards(1)">
              <option value="">全部版本</option>
              <option v-for="version in shardVersions" :key="version" :value="version">{{ version }}</option>
            </select>
          </label>
          <button class="btn" type="button" data-testid="shard-refresh" @click="reloadShards(shardPage)">刷新</button>
        </div>
      </header>
      <table class="data-table" data-testid="shard-table">
        <thead>
          <tr>
            <th>快照编号</th><th>快照版本</th><th>分片序号</th><th>分片行数/期望行数</th>
            <th>校验哈希/期望哈希</th><th>校验状态</th><th>校验结论</th><th>校验时间</th><th>来源模块</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="shard in shards" :key="String(shard.id)" :data-testid="`shard-${shard.id}`" :data-status="shard['校验结论']">
            <td>{{ shard['快照编号'] }}</td>
            <td>{{ shard['快照版本'] }}</td>
            <td>{{ shard['分片序号'] }}</td>
            <td>{{ shard['分片行数'] }} / {{ shard['期望行数'] }}</td>
            <td class="shard-hash">{{ shard['校验哈希'] || '—' }} / {{ shard['期望哈希'] || '—' }}</td>
            <td>{{ shard['校验状态'] }}</td>
            <td>{{ shard['校验结论'] }}</td>
            <td>{{ shard['校验时间'] || '—' }}</td>
            <td>{{ shard['来源模块'] || '—' }}</td>
          </tr>
          <tr v-if="!shards.length">
            <td colspan="9" class="empty-state">该快照没有分片校验记录（空态），等待作业写入分片结果</td>
          </tr>
        </tbody>
      </table>
      <footer class="page-foot">
        <span>共 {{ shardTotal }} 条分片校验记录，第 {{ shardPage }} 页</span>
        <span class="row-actions">
          <button class="link" type="button" :disabled="shardPage <= 1" @click="reloadShards(shardPage - 1)">上一页</button>
          <button class="link" type="button" :disabled="shardPage * shardSize >= shardTotal" @click="reloadShards(shardPage + 1)">下一页</button>
        </span>
      </footer>
    </section>

    <section class="review-zone" data-testid="review-zone">
      <h3>待核对区</h3>
      <p class="page-desc">中断恢复后的异常结果留在待核对区，核对完成后才移出，不会进入恢复清单。</p>
      <ul v-if="reviews.length">
        <li v-for="review in reviews" :key="String(review.id)">
          <strong>{{ review['快照编号'] }} · {{ review['版本'] }}</strong>
          — {{ review['类型'] }} · {{ review['状态'] }}
          <div class="review-reason">{{ review['原因'] }}</div>
        </li>
      </ul>
      <p v-else class="empty-state">暂无待核对记录</p>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记快照记录</h3>
        <label v-for="field in createFields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :placeholder="`请输入${field}`" :required="field === columns[0] || field === columns[1] || field === columns[2]" />
        </label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>

    <div v-if="showShardForm" class="modal-backdrop" data-testid="shard-modal">
      <form class="modal" @submit.prevent="submitShard">
        <h3>提交分片校验（{{ selectedItem?.name }} · {{ shardForm.version }}）</h3>
        <p class="page-desc">
          同一（快照编号, 版本, 分片序号）只保留一份有效结果；同一请求号重复提交不会重复写入。
        </p>
        <label v-for="field in shardFields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="shardForm[field]" :data-testid="`shard-${field}`" :required="field !== '校验状态'" />
        </label>
        <label class="create-field">
          <span>期望分片数（缺片必须显式声明，否则不允许完成）</span>
          <input v-model="shardForm.total" type="number" min="0" data-testid="shard-total" />
        </label>
        <label class="create-field">
          <span>发布为带修复标记的版本</span>
          <input v-model="shardForm.repair" type="checkbox" data-testid="shard-repair" />
        </label>
        <label class="create-field">
          <span>校验状态</span>
          <select v-model="shardForm['校验状态']" data-testid="shard-校验状态">
            <option value="通过">通过</option>
            <option value="失败">失败</option>
            <option value="无数据">无数据</option>
          </select>
        </label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showShardForm=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-shard">提交校验</button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ timelineItems.length }} 条快照作业记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, any>

const ENDPOINT = '/api/snapshot'
const columns = ['快照编号', '数据集名称', '快照时间', '行数快照', '校验哈希', '快照策略', '存储位置', '快照状态']
const actions = ['执行快照', '暂停快照', '标记失败']
const createFields = [...columns.slice(0, 3), '版本', '分片总数']
const shardFields = ['分片序号', '分片行数', '期望行数', '校验哈希', '期望哈希']

const timelineItems = ref<Row[]>([])
const allItems = ref<Row[]>([])
const selectedItem = ref<Row | null>(null)
const detail = ref<Row | null>(null)
const dateFrom = ref('')
const dateTo = ref('')
const errorMessage = ref('')
const actionMessage = ref('')

const showCreate = ref(false)
const createValues = ref<Record<string, string>>({})

const shards = ref<Row[]>([])
const shardTotal = ref(0)
const shardPage = ref(1)
const shardSize = 5
const shardVersion = ref('')
const shardVersions = ref<string[]>([])
const reviews = ref<Row[]>([])

const showShardForm = ref(false)
const shardForm = ref<Record<string, any>>({ version: '', total: '', repair: false, requestId: '', '校验状态': '通过' })

function barLeft(item: Row) {
  const all = allItems.value.filter((i) => i.start && i.end)
  if (!all.length) return 0
  const min = Math.min(...all.map((i) => new Date(i.start).getTime()))
  const max = Math.max(...all.map((i) => new Date(i.end).getTime()))
  const range = max - min || 86400000
  return ((new Date(item.start).getTime() - min) / range) * 100
}

function barWidth(item: Row) {
  const all = allItems.value.filter((i) => i.start && i.end)
  if (!all.length) return 20
  const min = Math.min(...all.map((i) => new Date(i.start).getTime()))
  const max = Math.max(...all.map((i) => new Date(i.end).getTime()))
  const range = max - min || 86400000
  return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100)
}

function reasonTone(reasonType: string) {
  if (reasonType === '分片失败') return 'error'
  if (reasonType === '无数据') return 'warn'
  if (reasonType === '空态') return 'muted'
  return 'ok'
}

async function selectItem(item: Row) {
  if (selectedItem.value?.id === item.id) {
    selectedItem.value = null
    detail.value = null
    return
  }
  selectedItem.value = item
  actionMessage.value = ''
  shardVersion.value = ''
  await loadDetail(item.id)
  await reloadShards(1)
}

async function loadDetail(id: number) {
  try {
    const response = await request(`${ENDPOINT}/${id}`)
    if (!response.ok) throw new Error('快照详情读取失败')
    detail.value = await response.json()
    reviews.value = detail.value?.['待核对记录'] || []
    shardVersions.value = Array.from(
      new Set([
        detail.value?.['版本'],
        detail.value?.['当前可恢复版本']?.['版本'],
        ...(detail.value?.['分片校验记录'] || []).map((row: Row) => row['快照版本']),
      ].filter(Boolean) as string[]),
    )
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '快照详情读取失败'
  }
}

async function reloadShards(page: number) {
  if (!selectedItem.value) return
  const target = Math.max(1, page)
  const params = new URLSearchParams({
    '快照编号': String(selectedItem.value.name),
    page: String(target),
    size: String(shardSize),
  })
  if (shardVersion.value) params.set('版本', shardVersion.value)
  try {
    const response = await request(`${ENDPOINT}/shards?${params.toString()}`)
    if (!response.ok) throw new Error('分片校验记录读取失败')
    const payload = await response.json()
    shards.value = payload.items ?? []
    shardTotal.value = payload.total ?? shards.value.length
    shardPage.value = payload.page ?? target
    // 版本清单以接口返回的全量版本为准，翻页时下拉不会丢版本。
    shardVersions.value = payload.versions ?? shardVersions.value
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分片校验记录读取失败'
  }
}

async function reload() {
  try {
    const response = await request(ENDPOINT)
    if (!response.ok) throw new Error('快照列表读取失败')
    const data = await response.json()
    allItems.value = data.items || []
    applyFilter()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '快照列表读取失败'
  }
}

function applyFilter() {
  let items = allItems.value
  if (dateFrom.value) items = items.filter((i) => i.start >= dateFrom.value)
  if (dateTo.value) items = items.filter((i) => i.end <= dateTo.value)
  const selectedId = selectedItem.value?.id
  timelineItems.value = items
  // 动作后刷新列表时保留当前选中项，详情面板不会闪没。
  const refreshed = selectedId == null ? null : items.find((i) => i.id === selectedId) || null
  selectedItem.value = refreshed
  if (!refreshed) detail.value = null
}

function openCreate() {
  createValues.value = {}
  showCreate.value = true
}

async function submitCreate() {
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST',
      body: JSON.stringify({ values: createValues.value }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  }
}

async function runAction(action: string) {
  if (!selectedItem.value) return
  actionMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${selectedItem.value.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    const payload = await response.json()
    if (!payload.ok) {
      actionMessage.value = payload.message || '动作未生效'
      return
    }
    actionMessage.value = payload.message
    await reload()
    await loadDetail(selectedItem.value.id)
  } catch (error) {
    actionMessage.value = error instanceof Error ? error.message : '动作未生效'
  }
}

async function recoverSelected(version?: string) {
  if (!selectedItem.value) return
  actionMessage.value = ''
  const query = version ? `?${new URLSearchParams({ 版本: version }).toString()}` : ''
  try {
    const response = await request(`${ENDPOINT}/${selectedItem.value.id}/recover${query}`, { method: 'POST' })
    const payload = await response.json()
    actionMessage.value = payload.ok
      ? `${payload.message}（分片 ${payload.entry['分片数']}，行数合计 ${payload.entry['行数合计']}）`
      : payload.message
    await reload()
    await loadDetail(selectedItem.value.id)
  } catch (error) {
    actionMessage.value = error instanceof Error ? error.message : '恢复失败'
  }
}

function openShardForm() {
  const version = detail.value?.['版本'] || selectedItem.value?.version || ''
  shardForm.value = {
    version,
    total: String(detail.value?.['分片总数'] ?? ''),
    repair: false,
    requestId: `req-${Date.now()}`,
    '校验状态': '通过',
    '分片序号': '',
    '分片行数': '',
    '期望行数': '',
    '校验哈希': '',
    '期望哈希': '',
  }
  showShardForm.value = true
}

async function submitShard() {
  if (!selectedItem.value) return
  errorMessage.value = ''
  const form = shardForm.value
  const shard: Row = {
    '分片序号': Number(form['分片序号']),
    '分片行数': Number(form['分片行数']),
    '期望行数': Number(form['期望行数']),
    '校验哈希': form['校验哈希'],
    '期望哈希': form['期望哈希'],
    '校验状态': form['校验状态'],
  }
  try {
    const response = await request(`${ENDPOINT}/${selectedItem.value.id}/shards/validate`, {
      method: 'POST',
      body: JSON.stringify({
        shards: [shard],
        version: form.version || undefined,
        total_shards: form.total === '' ? undefined : Number(form.total),
        repair: Boolean(form.repair),
        request_id: form.requestId,
      }),
    })
    const payload = await response.json()
    actionMessage.value = payload.message
    showShardForm.value = false
    await reload()
    await loadDetail(selectedItem.value.id)
    await reloadShards(shardPage.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分片校验提交失败'
  }
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

onMounted(reload)
</script>
