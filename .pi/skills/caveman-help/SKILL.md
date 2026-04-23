---
name: caveman-help
description: >
  Quick-reference card for all caveman modes, skills, and commands.
  One-shot display, not a persistent mode. Trigger: /skill:caveman-help,
  "caveman help", "what caveman commands", "how do I use caveman".
---

# Caveman Help

Display this reference card when invoked. One-shot — do NOT change mode, write flag files, or persist anything. Output in caveman style.

## Modes

| Mode | Trigger | What change |
|------|---------|-------------|
| **Lite** | `/skill:caveman lite` | Drop filler. Keep sentence structure. |
| **Full** | `/skill:caveman` | Drop articles, filler, pleasantries, hedging. Fragments OK. Default. |
| **Ultra** | `/skill:caveman ultra` | Extreme compression. Bare fragments. Tables over prose. |
| **Wenyan-Lite** | `/skill:caveman wenyan-lite` | Classical Chinese style, light compression. |
| **Wenyan-Full** | `/skill:caveman wenyan` | Full 文言文. Maximum classical terseness. |
| **Wenyan-Ultra** | `/skill:caveman wenyan-ultra` | Extreme. Ancient scholar on a budget. |

Mode stick until changed or session end.

## Skills

| Skill | Trigger | What it do |
|-------|---------|-----------| 
| **caveman-commit** | `/skill:caveman-commit` | Terse commit messages. Conventional Commits. ≤50 char subject. |
| **caveman-review** | `/skill:caveman-review` | One-line PR comments: `L42: bug: user null. Add guard.` |
| **caveman-help** | `/skill:caveman-help` | This card. |

## Deactivate

Say "stop caveman" or "normal mode". Resume anytime with `/skill:caveman`.

## More

Full docs: https://github.com/JuliusBrussee/caveman
