"""Fake application package referenced by the ``corpus_v1`` artifacts.

Made importable by the ``fake_myapp_path`` fixture so the ``module:symbol``
references in the corpus — output ``type_ref`` targets and ``python`` toolset
factories — resolve without a real application. Skills are *not* symbols: they
are ``SKILL.md`` packages shipped beside the artifact or under
``fixtures/skills_root``, so nothing here backs them.
"""
