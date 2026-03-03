# LeVision Design System

## Product
Basketball analytics platform that uses computer vision to annotate game footage
and break it down into quantifiable data. Named after LeBron James. Knows it.

## Personality
The UI is serious and precision-engineered. The microcopy is sharp, specific,
and sounds like someone who has followed LeBron's career very closely.
Never corny. Never generic sports-app enthusiasm. No exclamation points.
The joke lands because it's accurate, not because it's loud.

The 👑 emoji appears in exactly two places:
1. In the logo mark next to "LeVision"
2. On the final loaded/complete state
Nowhere else. Ever.

---

## Colors

| Token              | Value                      | Usage                              |
|--------------------|----------------------------|------------------------------------|
| `bg-pitch`         | #090b0e                    | Page backgrounds                   |
| `bg-surface`       | rgba(255, 255, 255, 0.03)  | Cards, panels, modals              |
| `text-offwhite`    | #f0ece4                    | Primary text                       |
| `text-muted`       | #6b6660                    | Labels, captions, hints            |
| `text-brand`       | #c8883a                    | Accent text, eyebrows, highlights  |
| `bg-brand`         | #c8883a                    | Primary buttons                    |
| `bg-brand-light`   | #e8a85a                    | Button hover states                |
| `text-crown`       | #f5a623                    | Reserved: Pro badges, achievements |
| `text-accent`      | #ff5533                    | Errors, destructive actions        |
| `border-border`    | rgba(200, 136, 58, 0.22)   | All borders                        |

---

## Typography

- **Display / Headings / Buttons / Eyebrows**: `font-display` → Bebas Neue
- **Body / Labels / Captions / Microcopy**: `font-body` → DM Sans (300, 400, 500)
- Heading line-height: `leading-[0.92]` or `leading-[0.95]`
- Eyebrow tracking: `tracking-[0.22em]`
- Label tracking: `tracking-[0.18em]`
- Button tracking: `tracking-widest`
- Body weight: 300 for most copy, 400 for emphasis, 500 for UI labels
- No Inter. No Roboto. No system fonts.

---

## Component Patterns

### Page Shell
Every page must include:
- `bg-pitch` base
- Court line SVG overlay: fixed, `opacity-15`, amber stroked, `pointer-events-none`, `z-0`
- Grain texture: fixed, `opacity-35`, `pointer-events-none`, `z-10`
- Radial amber glow: `radial-gradient` at bottom center, `rgba(200,136,58,0.10)`
- Scanning line: animated `h-px` from transparent → brand → transparent, top to bottom, 5s loop

### Cards / Panels
```
bg-surface border border-border rounded-sm backdrop-blur-md
relative overflow-hidden
::before → top amber line: linear-gradient(90deg, transparent, #c8883a, transparent), h-px
```

### Primary Button
```
bg-brand hover:bg-brand-light text-pitch font-display
tracking-widest rounded-sm px-9 py-3.5
transition-colors duration-200
```

### Ghost Button
```
text-muted hover:text-offwhite font-body text-sm
tracking-[0.06em] bg-transparent border-none
transition-colors duration-200
```

### Eyebrow
```
text-brand uppercase text-[0.68rem] tracking-[0.22em] font-medium font-body
```

### Display Heading
```
font-display text-offwhite leading-[0.92] tracking-[0.04em]
```
Accent word: wrap in `<em>` (not italic) or `<span>` with `text-brand`

### Input Field
```
bg-white/[0.04] border border-white/10 focus:border-brand
focus:bg-brand/5 rounded-sm px-4 py-3
text-offwhite font-body font-light text-sm
outline-none transition-colors duration-200
placeholder:text-white/20
```

### Chip / Tag
```
border border-brand/30 bg-brand/5 text-offwhite
text-[0.76rem] tracking-[0.07em] rounded-none px-4 py-2
flex items-center gap-2
```
With a 5×5px `bg-brand rounded-full` dot as the leading element.

### Progress / Step Indicator
```
flex gap-2
Each segment: flex-1 h-0.5 rounded-sm bg-brand/20
Active segment: bg-brand + shimmer animation
```

---

## Animations

### Page entrance
```css
@keyframes fadeUp {
  from { opacity: 0; transform: translateY(36px); }
  to   { opacity: 1; transform: translateY(0); }
}
```
Stagger children with `animation-delay` in 0.05–0.1s increments.

### Scan line
```css
@keyframes scan {
  0%   { top: -2px; }
  100% { top: 100vh; }
}
/* 5s ease-in-out infinite, opacity 0.4 */
```

### CV dots (tracking indicators)
```css
@keyframes ring {
  0%   { transform: scale(1); opacity: 0.4; }
  100% { transform: scale(2.8); opacity: 0; }
}
```

### Progress shimmer
```css
@keyframes shimmer {
  0%   { transform: translateX(-100%); }
  100% { transform: translateX(100%); }
}
```

---

## Microcopy

Use these verbatim. Do not invent new copy. It should sound like someone
who has followed LeBron's career closely — specific, dry, occasionally sharp.

| Context                | Copy                                                                          |
|------------------------|-------------------------------------------------------------------------------|
| Loading (1)            | "Cross-referencing with LeBron's 11pm film session…"                         |
| Loading (2)            | "Longer than LeBron's letter to Cleveland. Almost."                          |
| Loading (3)            | "Crunching numbers. LeBron is already on the next play."                     |
| Loading done           | "Ready. LeBron would've watched this twice by now. 👑"                       |
| Empty (no film)        | "No footage yet. LeBron didn't become LeBron by skipping film."              |
| Empty (no plays)       | "No plays saved. Even Phil Jackson wrote things down."                       |
| Empty (no data)        | "Nothing here. Emptier than Cleveland's trophy case before 2016."            |
| Error (upload)         | "Upload failed. Blame the refs."                                             |
| Error (general)        | "Something broke. Not the hairline though."                                  |
| Error (timeout)        | "Timed out. LeBron's patience with bad teammates has a limit too."           |
| Success (upload)       | "Footage locked in. The film doesn't lie."                                   |
| Success (saved)        | "Saved. Somewhere, LeBron approves."                                         |
| Sign in subtext        | "LeBron has watched more film than every player in league history. This is where you catch up." |
| Welcome body           | "You now have access to the same obsessive film breakdown LeBron's been doing since he was 16." |
| Role select subtitle   | "Even The King had Spoelstra."                                               |
| Upload prompt          | "Drop your footage. We'll do what LeBron does at midnight."                  |
| CV engine ready        | "CV Engine ready. Unlike Cleveland's defense in Game 6."                     |
| No team added          | "No team added yet. LeBron carried less and still won."                      |
| Onboarding skip        | "Skip. Bold choice. LeBron never skips film."                                |
| Upgrade/Pro prompt     | "LeBron didn't take the minimum. Don't limit your analytics."                |
| Confirm delete         | "Are you sure? Even LeBron regrets leaving Cleveland. Twice."                |

---

## What to Always Avoid

- Inter, Roboto, or any system font
- Purple gradients, blue gradients, or any generic SaaS palette
- Rounded pill buttons (`rounded-full`)
- Exclamation points in microcopy
- Generic empty states ("No data found", "Nothing here yet")
- Emoji outside the two designated 👑 placements
- Any UI that could belong to a different product
- `border-radius` larger than `rounded-sm` on core UI elements
- White or light backgrounds on any page or panel

---

## How to Prompt Claude Code

**Every new component:**
> Refer to LEVISION_DESIGN_SYSTEM.md for all design, color, typography, and copy decisions. Use Tailwind 4 utility classes only. Do not introduce new colors, fonts, or microcopy outside the design system.

**For a new page:**
> Build this as a full page using the LeVision page shell pattern from LEVISION_DESIGN_SYSTEM.md — bg-pitch base, court line SVG overlay, grain texture, scanning line animation, and radial amber glow.

**For empty/error/loading states:**
> Use the exact microcopy from the table in LEVISION_DESIGN_SYSTEM.md. Match the context to the closest entry. Do not write new copy.

**For any component with text:**
> No exclamation points. No emoji except the two designated 👑 placements defined in the design system.
