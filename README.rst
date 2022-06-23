========
pushglob
========

pushglob wraps the Globus CLI and maintains a database of files on a
local system.  It assists with pushing updates out to other Globus
endpoints.  It currently only works in a mode where data are generated
on one system and then pushed out to one or more satellite systems.
There is no way to track changes back from the satellite systems.

Installation
============

This package depends on globus-cli and PyYAML, so make sure those are installed::

  pip install globus-cli PyYAML

Then install this package, from the source tree::

  pip install .

Config file
===========

An empty config file will be created automatically if you run
``pushglob``.  It will go to ~/.pushglob, by default, but you can
change where pushglob looks for config by setting the PUSHGLOB_CONFIG
environment variable to the filename you want.

A .sqlite database will also be created, in the path specified in the
config file, so update the config file and run ``pushglob`` again to
create the sqlite file in the right place.

To do any transfers, you need to configure "endpoints" and "spaces" in
the config file.


Adding Endpoints
================

Endpoints correspond to globus endpoints; you can discover endpoint
IDs using the globus web interface or through the CLI using a search
function.  Often all you'll need is this::

  $ globus endpoint search --filter-scope=recently-used

That should print out a list of long ID codes.  E.g.::

  ID                                   | Owner                  | Display Name     
  ------------------------------------ | ---------------------- | -----------------
  a98234780-weird-hex-uuid22000b92c6ec | example1@globusid.org  | Example 1
  8defa9091-weird-hex-uuid3201e8c001d1 | example2@globusid.org  | Example 2

Add the IDs you want to the "globus_endpoints" part of the config
file, with a convenient label.  E.g::

  globus_endpoints:
    system-name1: 'a98234780-weird-hex-uuid22000b92c6ec'
    system-name2: '8defa9091-weird-hex-uuid3201e8c001d1'

One of these needs to correspond to the endpoint on the local machine.
Update "local_endpoint" to match the globus_endpoint nickname::

  local_endpoint: system-name1

You can test that the endpoints are accessible by running::

  pushglob test

This will try to execute "globus ls" on each endpoint.

  
Adding Spaces
=============

A "space" is a tree that you want to keep synced (to some extent) on
other systems.  All you need to do for this is declare the path of the
"space" on the local system and on each system you want to push data
to.  Following the example above::

  spaces:
    projectA_data:
      system-name1: '/home/me/projectA'
      system-name2: '/projects/my_group/me/projectA'
    projectB_data:
      system-name1: '/some/shared/folder/B'
      system-name2: '/projects/my_group/me/projectB'

Triggering a transfer
=====================

Request a transfer of an entire space::

  pushglob sync system-name2 -s projectA_data

When you first run this, it will try to transfer everything in the
space "projectA_data" to the endpoint called "system-name2".  It
doesn't know whether or not there are already remote copies of some of
these files, so it copies everything. But subsequent to that first
run, it will remember transfers you've done in the past and only
request transfers for files that are new or modified.

You can request scan + sync of only particular subdirectories in a
space.  Use this form::

  pushglob sync system-name2 /projects/my_group/me/projectA/active_area 

The code will figure out that the "active_area" directory is part of
the space called "projectA_data", and will only analyze and transfer
files from that directory.
