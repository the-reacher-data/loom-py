# Feature Spec — Documentation Platform + Docs Pipeline
## Plan de Implementación por Piezas

**Fecha**: 2026-02-27  
**Diseño base**: `specs/documentation-platform.md` + `specs/v2/07_docs_platform.md` + `specs/v2/09_docs_platform_execution.md`  
**Workflow por pieza**: Arquitectura -> Implementación -> Testing -> Review -> Commit  
**Si review falla**: volver a Arquitectura de la pieza afectada antes de continuar.

---

## Objetivo de producto

Construir una plataforma de documentación de alto nivel para `loom-kernel` que combine:

1. **Documentación autogenerada** desde docstrings de API pública (`src/loom`).
2. **Documentación editorial** escrita por el equipo en `docs/` (guías, arquitectura, tutoriales, ADRs).
3. **Pipeline CI/CD de docs** reproducible y estricto (warnings como errores).

Resultado esperado: docs mantenibles, versionables y publicables sin pasos manuales ambiguos.

---

## Índice de piezas

| # | Pieza | Archivos principales | Dependencias |
|---|---|---|---|
| 0 | Decisiones base y alcance | `specs/feature/docs.md` | ninguna |
| 1 | Bootstrap técnico de Sphinx | `docs/conf.py`, `docs/index.rst`, `docs/_templates/`, `docs/_static/` | 0 |
| 2 | Estructura editorial de `docs/` | `docs/guides/*`, `docs/architecture/*`, `docs/reference/index.rst` | 1 |
| 3 | API reference autogenerada | `docs/reference/api/*`, scripts/autosummary config | 1, 2 |
| 4 | Dependencias docs en proyecto | `pyproject.toml` | 1 |
| 5 | Pipeline CI de docs (PR + master) | `.github/workflows/docs.yml` | 1, 3, 4 |
| 6 | Publicación Read the Docs | `.readthedocs.yaml` | 1, 3, 4 |
| 7 | Integración README y badges | `README.md` | 5, 6 |
| 8 | Quality gates docs avanzados | workflow docs + checks de enlaces/ejemplos | 5, 6, 7 |
| 9 | Snyk coverage completa en CI | workflow security/snyk + export temporal de manifests | 4 |

---

## Pieza 0 — Decisiones base y alcance

### Arquitectura

Definir contrato de documentación:

- **Fuente única de API**: docstrings de módulos públicos.
- **Fuente editorial**: archivos en `docs/`.
- **No acoplar dominio a tooling**: Sphinx/RTD solo en capa de documentación y CI.
- **Regla de calidad**: docs deben compilar sin warnings (`-W`).

### Implementación

- Consolidar decisiones en este spec (stack, estructura, gates, fases).
- Definir política de ramas:
  - PR valida docs.
  - merge a `master` habilita publicación.

### Testing

- Revisión por checklist (sin cambios de código aún).

### Review checklist

- [ ] Alcance y exclusiones definidos.
- [ ] Criterios de aceptación medibles.
- [ ] Orden de piezas viable para commits incrementales.

---

## Pieza 1 — Bootstrap técnico de Sphinx

### Arquitectura

Sphinx como motor principal por soporte robusto de autodoc/autosummary/napoleon.

### Implementación

Crear base mínima:

```text
docs/
  conf.py
  index.rst
  _templates/
  _static/
```

Config inicial:

- `extensions`:
  - `sphinx.ext.autodoc`
  - `sphinx.ext.autosummary`
  - `sphinx.ext.napoleon`
  - `sphinx.ext.viewcode`
  - `sphinx.ext.intersphinx`
- `autosummary_generate = True`
- `napoleon_google_docstring = True`
- tema inicial estable (Furo o equivalente).

### Testing

- `sphinx-build -W -b html docs docs/_build/html` local.

### Review checklist

- [ ] Build local sin warnings.
- [ ] Navegación base funciona.
- [ ] Config tipada y mantenible en `conf.py`.

---

## Pieza 2 — Estructura editorial de `docs/`

### Arquitectura

Separar claramente:

- **Guides**: onboarding y uso.
- **Architecture**: decisiones y diseño.
- **Reference**: API.

### Implementación

Estructura mínima:

```text
docs/
  guides/
    quickstart.md
    fake-repo-examples.md
    autocrud.md
  architecture/
    overview.md
    clean-architecture.md
    adr/
      README.md
  reference/
    index.rst
```

Contenido inicial obligatorio:

- propósito de la librería;
- mapa de subrutas;
- tutorial corto de `UseCase + Rule + Compute`;
- caso AutoCRUD real.

### Testing

- Build docs en estricto.
- Revisión manual de enlaces internos.

### Review checklist

- [ ] El contenido editorial no duplica API reference literal.
- [ ] Guías orientadas a tareas reales del usuario.
- [ ] Estructura preparada para crecer por módulos.

---

## Pieza 3 — API reference autogenerada

### Arquitectura

Generación automática desde `src/loom` con exclusión explícita de internals no públicos.

### Implementación

- Crear `docs/reference/api/` con índices por submódulo:
  - `core`
  - `rest`
  - `testing`
  - `prometheus`
- Usar `autosummary` + `autodoc` y `toctree` dedicado.
- Definir política de exposición:
  - solo símbolos públicos;
  - módulos internos fuera del árbol principal.

### Testing

- Build estricto.
- Verificar que la API principal aparece y no se rompen imports en docs.

### Review checklist

- [ ] API reference se regenera sin intervención manual.
- [ ] Sin imports accidentales de infraestructura pesada.
- [ ] Nombres y rutas consistentes con `__init__` públicos.

---

## Pieza 4 — Dependencias docs en proyecto

### Arquitectura

Dependencias de docs deben ser opcionales y aisladas del runtime core.

### Implementación

En `pyproject.toml`, crear grupo/extra de docs (sin contaminar deps base):

- `sphinx`
- `furo` (o tema elegido)
- `myst-parser`
- `sphinx-autodoc-typehints` (si aporta claridad)

### Testing

- instalación limpia del grupo docs;
- build docs en entorno limpio CI.

### Review checklist

- [ ] `project.dependencies` permanece mínima.
- [ ] docs tooling en grupo/extra específico.
- [ ] versión fijada con rangos razonables.

---

## Pieza 5 — Pipeline CI de docs

### Arquitectura

Validar docs en cada PR para evitar drift entre código y documentación.

### Implementación

Crear `.github/workflows/docs.yml`:

- trigger:
  - `pull_request` (paths docs/src/README/pyproject)
  - `push` a `master`
  - `workflow_dispatch`
- pasos:
  - setup python
  - instalar deps docs
  - `sphinx-build -W -b html docs docs/_build/html`
  - opcional: subir artifact HTML

### Testing

- ejecutar workflow en PR real;
- verificar fallo con warning intencional.

### Review checklist

- [ ] El workflow falla correctamente ante warnings.
- [ ] Tiempo de ejecución razonable.
- [ ] Paths filter evita ejecuciones innecesarias.

---

## Pieza 6 — Publicación en Read the Docs

### Arquitectura

Publicación desacoplada del build de app; versionado por ramas/tags.

### Implementación

Crear `.readthedocs.yaml`:

- `version: 2`
- `sphinx.configuration: docs/conf.py`
- instalación desde `pyproject.toml` con deps docs
- rama por defecto: `master`

### Testing

- validar primer build en RTD;
- comprobar URLs de versión `latest` y `stable` según estrategia.

### Review checklist

- [ ] RTD compila sin pasos manuales ocultos.
- [ ] Config coherente con workflow CI.
- [ ] Ramas/versiones alineadas con release process.

---

## Pieza 7 — Integración README y badges

### Arquitectura

README como puerta de entrada; docs site como fuente extensa.

### Implementación

- Añadir badge de docs.
- Añadir enlaces directos a:
  - quickstart;
  - API reference;
  - guía de arquitectura.
- Mantener README breve; detalle profundo en `docs/`.

### Testing

- validar links;
- revisar render en GitHub.

### Review checklist

- [ ] README y docs no se contradicen.
- [ ] Badges y enlaces operativos.
- [ ] Ejemplos del README apuntan a guías vivas.

---

## Pieza 8 — Quality gates docs avanzados

### Arquitectura

Asegurar que la calidad escale con el proyecto.

### Implementación

Gates adicionales (fase incremental):

- chequeo de enlaces rotos;
- verificación de snippets críticos;
- cobertura mínima de docstrings en módulos públicos críticos.

### Testing

- introducir fallo controlado por gate y validar que CI bloquea.

### Review checklist

- [ ] Gates tienen bajo ruido y alto valor.
- [ ] Los falsos positivos son manejables.
- [ ] Política de excepción documentada.

---

## Pieza 9 — Snyk coverage completa en CI (sin manifests versionados)

### Arquitectura

Sí. La action puede **generar manifests temporales** durante CI (en `/tmp` o workspace efímero) y ejecutar Snyk sobre esos archivos, sin commitearlos al repositorio.

### Implementación propuesta (CI)

1. Exportar dependencias por grupos/extras en runtime:
   - core
   - dev
   - extras (`fastapi`, `sqlalchemy`, `cache`, etc.)
2. Ejecutar `snyk test`/`snyk monitor` por cada archivo exportado.
3. Definir `--project-name` explícito para que el panel quede ordenado.

### Cómo aparece en Snyk si se separa

Se verán **múltiples proyectos** en la organización, por ejemplo:

- `loom-py/core`
- `loom-py/dev`
- `loom-py/extra-fastapi`
- `loom-py/extra-sqlalchemy`
- `loom-py/extra-cache`

Ventaja: vulnerabilidades segmentadas por contexto de uso, menos ruido y ownership más claro.

### Testing

- Ejecutar workflow en PR y comprobar que sube todos los proyectos esperados.
- Introducir dependencia vulnerable en un extra de prueba para validar detección segmentada.

### Review checklist

- [ ] No se añaden `requirements*.txt` persistentes al repo.
- [ ] La action genera y usa manifests temporales.
- [ ] Snyk muestra proyectos separados y nombrados de forma estable.

---

## Criterios de aceptación globales

1. Existe `docs/` con estructura editorial y API reference autogenerada.  
2. CI falla si docs no compilan con `-W`.  
3. Publicación automática en Read the Docs tras merge a `master`.  
4. README enlaza docs públicas y rutas clave.  
5. El sistema combina correctamente contenido autogenerado + contenido manual.

---

## Riesgos y mitigaciones

- **Drift docs-código**: mitigar con `-W` + checks de snippets en CI.
- **Sobrecarga de mantenimiento**: plantillas reutilizables y guía editorial corta.
- **Dependencias docs inestables**: fijar rangos y revisar trimestralmente.
- **Ambigüedad de versión publicada**: política explícita `latest/stable` en RTD.

---

## Plan de commits por fases

1. `docs(spec): add phased plan for documentation platform` (este archivo)  
2. `docs(infra): bootstrap sphinx docs skeleton`  
3. `docs(content): add guides architecture and reference index`  
4. `docs(api): enable autosummary autogenerated reference`  
5. `build(docs): add docs dependency group to pyproject`  
6. `ci(docs): add strict docs workflow`  
7. `ci(docs): add readthedocs configuration`  
8. `docs(readme): link public docs and badges`  
9. `ci(docs): add advanced docs quality gates`
10. `ci(security): generate ephemeral manifests and split snyk projects by dependency scope`

---

## Fuera de alcance (esta iteración)

- Tutoriales avanzados de ETL/Celery/Kafka completos.
- Migración total de todos los specs legacy a formato docs final.
- Portal de documentación multi-idioma.
