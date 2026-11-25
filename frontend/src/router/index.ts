import { createRouter, createWebHistory } from 'vue-router'

import FieldDefinitionsView from '@/views/FieldDefinitionsView.vue'
import FormatTrialView from '@/views/FormatTrialView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/format-trials', name: 'format-trials', component: FormatTrialView },
    { path: '/field-definitions', name: 'field-definitions', component: FieldDefinitionsView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
