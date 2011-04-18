Transaction Binding
=============

In my travels across the [Alfresco](www.alfresco.com) landscape, I
came across a nice piece of functionality buried deep inside some
of the core transaction handling API's.  It provides the ability to
bind an arbitrary object to the currently running transaction and
clean up any references to it after committing or rolling back.
The aim of this project is to extract and expose this functionality
with minimal external dependencies.
