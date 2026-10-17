# Score d'impact composite

```
impact = 100 × (0.5 × cit_norm + 0.3 × hn_norm + 0.2 × recency)
```

| Composante | Calcul | Pourquoi |
|---|---|---|
| `cit_norm` | percent_rank de `log1p(citations)` **par catégorie** | Les volumes de citations varient énormément selon les domaines ; le log écrase les valeurs extrêmes |
| `hn_norm` | percent_rank de `log1p(score + commentaires)` parmi les articles présents sur HN, 0 sinon | Signal de popularité tech, en log comme demandé |
| `recency` | `exp(-ln2 × âge / 2)` (demi-vie 2 ans) | Évite de pénaliser les articles récents qui n'ont pas encore eu le temps d'être cités |

## Pondérations

*À justifier : 0.5 / 0.3 / 0.2 (le signal académique reste le principal). Ajouter une analyse de
sensibilité (classement top 100 avec d'autres jeux de poids, corrélation de Spearman).*

## Cas d'usage (champs booléens dans ES)

- `is_hidden_gem` : `cit_norm ≥ 0.9` et jamais sur HN
- `is_hn_hype` : `hn_norm ≥ 0.9` et `cit_norm ≤ 0.3`
