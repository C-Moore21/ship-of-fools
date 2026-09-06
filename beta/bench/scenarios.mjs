// Click scenarios. Each measures one user interaction end to end.
//
//   prepare(page)  puts the UI in the pre-click state (NOT measured)
//   act(page)      performs the click (the measurement window wraps this)
//   until(page)    the real "this interaction finished" predicate
//   cleanup(page)  optional teardown
//
// `until` matters more than it looks. Clicking a show repaints the row
// highlight within ~10ms and then the DOM goes quiet for several hundred ms
// while three round trips resolve. Without a predicate that waits for the
// *content*, the harness happily reports 14ms for a half-second interaction.

const YEARS = 'nav[aria-label="Years"] button'
const SHOWS = 'section[aria-label^="Shows in"] ul li button'
const TABS = 'nav[aria-label="Sections"] button'
const TOOLS = 'nav[aria-label="Tools"] button'
const SET_SECTIONS = 'section[aria-labelledby^="set-"]'
const TRACKS = `${SET_SECTIONS} ul li button`
const SEARCH_BAR = 'header button:has(kbd)'

// A fingerprint of the detail pane. `until` waits for this to actually change,
// which is the only reliable signal that the clicked show finished rendering.
async function detailSignature(page) {
  return page.evaluate(() => {
    const h1 = document.querySelector('h1')?.textContent?.trim() ?? ''
    const rows = document.querySelectorAll('section[aria-labelledby^="set-"] ul li button')
    const first = rows[0]?.textContent?.trim().slice(0, 60) ?? ''
    const src = document.querySelector('[aria-haspopup="listbox"]')?.textContent?.trim() ?? ''
    return `${h1}|${rows.length}|${first}|${src}`
  })
}

async function gotoBrowseTab(page) {
  const browse = page.locator(TABS, { hasText: 'Browse' }).first()
  if ((await browse.getAttribute('aria-current')) !== 'page') await browse.click()
  await page.waitForSelector(YEARS, { timeout: 15000 })
}

// Select a year by its label so the target is explicit rather than an index
// into a list whose ordering could change.
async function pickYear(page, year) {
  await page.locator(YEARS, { hasText: String(year) }).first().click()
  await page.waitForSelector(`section[aria-label="Shows in ${year}"] ul li button`, { timeout: 15000 })
  await page.waitForTimeout(250)
}

function yearScenario(id, year, parkYear, description) {
  return {
    id,
    description,
    async prepare(page) {
      await gotoBrowseTab(page)
      await pickYear(page, parkYear)
    },
    async act(page) {
      await page.locator(YEARS, { hasText: String(year) }).first().click({ noWaitAfter: true })
    },
    // The list's aria-label carries the year, so this is unambiguous.
    async until(page) {
      return (await page.locator(`section[aria-label="Shows in ${year}"] ul li button`).count()) > 0
    },
  }
}

export const SCENARIOS = [
  yearScenario('year-click', 1977, 1972, 'Click a year in the rail -> show list repaints'),
  yearScenario('year-click-dense', 1969, 1995, 'Click a heavy year (largest show list render)'),

  {
    id: 'show-click-cold',
    description: 'Click a show never opened this session -> detail + setlist render',
    async prepare(page) {
      await gotoBrowseTab(page)
      await pickYear(page, 1977)
      await page.locator(SHOWS).first().click()
      await page.waitForSelector(TRACKS, { timeout: 15000 })
      await page.waitForTimeout(400)
      // Walk a fresh row each rep so nothing is served from the module cache.
      const n = await page.locator(SHOWS).count()
      this._i = (this._i ?? 0) + 1
      this._target = 1 + (this._i % Math.max(1, n - 1))
      this._before = await detailSignature(page)
    },
    async act(page) {
      await page.locator(SHOWS).nth(this._target).click({ noWaitAfter: true })
    },
    async until(page) {
      const now = await detailSignature(page)
      return now !== this._before && (await page.locator(TRACKS).count()) > 0
    },
  },

  {
    id: 'show-click-warm',
    description: 'Re-click an already-fetched show -> pure client render cost',
    async prepare(page) {
      await gotoBrowseTab(page)
      await pickYear(page, 1977)
      // Warm both rows, then park on row 0 so the measured click hits row 1 warm.
      await page.locator(SHOWS).nth(1).click()
      await page.waitForSelector(TRACKS, { timeout: 15000 })
      await page.waitForTimeout(500)
      await page.locator(SHOWS).nth(0).click()
      await page.waitForTimeout(700)
      this._before = await detailSignature(page)
    },
    async act(page) {
      await page.locator(SHOWS).nth(1).click({ noWaitAfter: true })
    },
    async until(page) {
      const now = await detailSignature(page)
      return now !== this._before && (await page.locator(TRACKS).count()) > 0
    },
  },

  {
    id: 'tab-stats',
    description: 'Switch to Stats tab -> lazy chunk + panel render',
    async prepare(page) {
      await gotoBrowseTab(page)
      await page.waitForTimeout(250)
    },
    async act(page) {
      await page.locator(TABS, { hasText: 'Stats' }).first().click({ noWaitAfter: true })
    },
    // Panel is mounted once the three-column browse layout is gone and the
    // Suspense fallback has been replaced.
    async until(page) {
      if (await page.locator(YEARS).first().isVisible().catch(() => false)) return false
      const txt = await page.evaluate(() => document.body.innerText)
      return !/^\s*$/.test(txt) && !txt.includes('Loading')
    },
  },

  {
    id: 'tab-back-to-browse',
    description: 'Return to Browse from a section tab -> three-column remount',
    async prepare(page) {
      await gotoBrowseTab(page)
      await page.locator(TABS, { hasText: 'Stats' }).first().click()
      await page.waitForTimeout(900)
    },
    async act(page) {
      await page.locator(TABS, { hasText: 'Browse' }).first().click({ noWaitAfter: true })
    },
    async until(page) {
      return (await page.locator(SHOWS).count()) > 0
    },
  },

  {
    id: 'open-search',
    description: 'Open the search palette from the header',
    async prepare(page) {
      await page.keyboard.press('Escape')
      await page.waitForTimeout(200)
      await gotoBrowseTab(page)
      await page.waitForSelector('[role="dialog"]', { state: 'detached', timeout: 5000 }).catch(() => {})
    },
    async act(page) {
      // At >=lg the icon launcher is `lg:hidden`; the desktop control is the
      // search bar carrying the `/` kbd hint.
      await page.locator(SEARCH_BAR).click({ noWaitAfter: true })
    },
    async until(page) {
      return page.locator('[role="dialog"] input').isVisible().catch(() => false)
    },
    async cleanup(page) {
      await page.keyboard.press('Escape')
      await page.waitForTimeout(200)
    },
  },

  {
    id: 'open-observatory',
    description: 'Open the Observatory modal (heaviest lazy chunk)',
    async prepare(page) {
      await page.keyboard.press('Escape')
      await page.waitForTimeout(200)
      await gotoBrowseTab(page)
      await page.waitForSelector('[role="dialog"]', { state: 'detached', timeout: 5000 }).catch(() => {})
    },
    async act(page) {
      await page.locator(`${TOOLS}[aria-label="Observatory"]`).click({ noWaitAfter: true })
    },
    async until(page) {
      // Mounting the dialog shell is not the interaction finishing — wait for
      // the heatmap grid to have real cells in it.
      return page.evaluate(() => {
        const d = document.querySelector('[role="dialog"]')
        return !!d && d.querySelectorAll('*').length > 150
      })
    },
    async cleanup(page) {
      await page.keyboard.press('Escape')
      await page.waitForTimeout(200)
    },
  },
]

export const SCENARIO_IDS = SCENARIOS.map((s) => s.id)
