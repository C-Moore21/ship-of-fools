export const manifest = {
  screens: {
    scr_i3w0o0: { name: "Barton Hall 5.08.77", route: "/", state: { "year": 1977, "showId": "1977-05-08" }, position: { "x": 4360, "y": 1820 } },
    scr_m8ok10: { name: "Boston Garden 5.07.77", route: "/", state: { "year": 1977, "showId": "1977-05-07" }, position: { "x": 2960, "y": 1820 } },
    scr_0n53qf: { name: "New Haven 5.05.77 (AUD)", route: "/", state: { "year": 1977, "showId": "1977-05-05" }, position: { "x": 1560, "y": 1820 } },
    scr_wzkcui: { name: "Palladium 5.04.77 (Debut)", route: "/", state: { "year": 1977, "showId": "1977-05-04" }, position: { "x": 160, "y": 1820 } },
    scr_169koi: { name: "St. Paul 5.11.77 (Final)", route: "/", state: { "year": 1977, "showId": "1977-05-11" }, position: { "x": 5760, "y": 1820 } },
    scr_gabzng: { name: "Winterland 6.09.77", route: "/", state: { "year": 1977, "showId": "1977-06-09" }, position: { "x": 7160, "y": 1820 } },
    scr_uxbmgd: { name: "Empty year — 1980", route: "/", state: { "year": 1980, "showId": "1977-05-08" }, position: { "x": 0, "y": 0 }, isDefaultRow: true }
  },
  sections: {
    sec_uc930y: { name: "1977 Tour", x: 0, y: 1600, width: 8520, height: 1180 }
  },
  layers: [
  { kind: "screen", id: "scr_uxbmgd" },
  { kind: "section", id: "sec_uc930y", children: [
    { kind: "screen", id: "scr_wzkcui" },
    { kind: "screen", id: "scr_0n53qf" },
    { kind: "screen", id: "scr_m8ok10" },
    { kind: "screen", id: "scr_i3w0o0" },
    { kind: "screen", id: "scr_169koi" },
    { kind: "screen", id: "scr_gabzng" }]
  }]

};