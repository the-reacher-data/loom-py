loom-kernel documentation
=========================

.. raw:: html

   <div class="loom-hero">
     <img src="_static/logo-transparent.png" alt="loom-kernel logo" />
   </div>
   <p align="center" style="margin:0.5rem 0 1.5rem;">
     <a href="https://github.com/the-reacher-data/loom-py/actions/workflows/ci-main.yml">
       <img src="https://img.shields.io/github/actions/workflow/status/the-reacher-data/loom-py/ci-main.yml?branch=master&label=ci" alt="CI" />
     </a>
     &nbsp;
     <a href="https://github.com/the-reacher-data/loom-py/actions/workflows/docs.yml">
       <img src="https://img.shields.io/github/actions/workflow/status/the-reacher-data/loom-py/docs.yml?branch=master&label=docs" alt="Docs" />
     </a>
     &nbsp;
     <a href="https://sonarcloud.io/summary/new_code?id=the-reacher-data_loom-py">
       <img src="https://sonarcloud.io/api/project_badges/measure?project=the-reacher-data_loom-py&metric=alert_status" alt="Quality Gate" />
     </a>
     &nbsp;
     <a href="https://sonarcloud.io/summary/new_code?id=the-reacher-data_loom-py">
       <img src="https://sonarcloud.io/api/project_badges/measure?project=the-reacher-data_loom-py&metric=security_rating" alt="Security" />
     </a>
     &nbsp;
     <a href="https://app.codecov.io/gh/the-reacher-data/loom-py/tree/master">
       <img src="https://codecov.io/gh/the-reacher-data/loom-py/branch/master/graph/badge.svg" alt="Coverage" />
     </a>
     &nbsp;
     <a href="https://pypi.org/project/loom-kernel/">
       <img src="https://img.shields.io/pypi/v/loom-kernel" alt="PyPI" />
     </a>
     &nbsp;
     <img src="https://img.shields.io/badge/python-3.11%2B-blue" alt="Python" />
   </p>

Framework-agnostic toolkit to build backend applications with typed use cases,
repository contracts, and transport adapters.

Official hosted documentation:
`loom-py.readthedocs.io <https://loom-py.readthedocs.io/en/latest/>`_.
Companion demo application:
`dummy-loom <https://github.com/the-reacher-data/dummy-loom>`_.

.. toctree::
   :maxdepth: 2
   :caption: Guides

   guides/etl
   guides/autocrud
   guides/quickstart
   examples-repo/index
   guides/use-case-dsl
   guides/celery
   guides/fake-repo-examples

.. toctree::
   :maxdepth: 2
   :caption: Architecture

   architecture/overview
   architecture/clean-architecture
   architecture/adr/README

.. toctree::
   :maxdepth: 2
   :caption: Reference

   reference/index
