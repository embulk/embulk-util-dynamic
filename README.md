embulk-util-dynamic
====================

A clone of [`embulk-core`'s `org.embulk.spi.util.Dynamic*` classes](https://github.com/embulk/embulk/blob/v0.10.30/embulk-core/src/main/java/org/embulk/spi/util/DynamicPageBuilder.java) to do the same job on Embulk plugin's side.

Versions
---------

Note that v0.1.1 and v0.2.0 are incompatible.

| embulk-util-dynamic | Embulk |
| ---- | ---- |
| v0.1.1 | It will stop working during Embulk v1.0 for the removal of `msgpack-java`. |
| v0.2.0 | It works only after Embulk v0.10.42. |

For Embulk plugin developers
-----------------------------

* [Javadoc](https://dev.embulk.org/embulk-util-csv/)

For Maintainers
----------------

### Release

Modify `version` in `build.gradle` at a detached commit to bump up the versions of Embulk standard plugins.

```
git checkout --detach master

(Edit: Remove "-SNAPSHOT" in "version" in build.gradle.)

git add build.gradle

git commit -m "Release vX.Y.Z"

git tag -a vX.Y.Z

(Edit: Write a tag annotation in the changelog format.)

git push -u origin vX.Y.Z  # Pushing a version tag would trigger a release operation on GitHub Actions after approval.
```
