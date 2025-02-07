#!/usr/bin/env python3

import dataclasses
from dataclasses import dataclass
from enum import StrEnum
from typing import Generic, TypeVar, Union

from packaging.version import Version, parse as parse_version
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet

# NOTE: This scheme does _not_ support version Epochs or _ranges_ based on
# suffixes such as local, pre, or post-release strings. However, it _does_ allow
# for exact version string matching via `===`, and it should generally work for
# "standard" release version triples (even when minor/patch are missing).


def main():
    valid_package_list = [
        "requests>2.28.0",
        "numpy>1.0",
        "scipy>1.0",
        "pandas",
    ]

    invalid_package_list = [
        "requests>2.28.0",
        "requests<2.10",
        # "numpy==1.0.*",
        "numpy==1.0.post1",
        "numpy==1.1.1",
    ]

    # This should pass.
    check_validity(valid_package_list)
    # This should raise.
    check_validity(invalid_package_list)


def check_validity(ps: list[str]):
    reqs = [Requirement(p) for p in ps]
    specs = {}
    for req in reqs:
        name = req.name
        old_specs = specs.get(name, SpecifierSet())
        new_specs = old_specs & req.specifier
        specs[name] = new_specs
    broken = []
    for name, spec_set in specs.items():
        if not is_consistent(spec_set):
            broken.append((name, spec_set))
    if len(broken) > 0:
        raise Exception(f"inconsistent package specifiers: {broken}")


class VersionOp(StrEnum):
    COMPATIBLE = "~="
    MATCHING = "=="
    EXCLUDING = "!="
    LEQ = "<="
    GEQ = ">="
    LT = "<"
    GT = ">"
    EQUAL = "==="


# Unfortunately, it doesn't seem to be possible to define the operator map or
# parse function _within_ VersionOp itself. It might be possible to do this with
# a decorator on the class itself that wraps the inner enum definition with a
# lookup table and parser, but that's harder to read and understand.
_VERSION_OPS = {op.value: op for op in VersionOp}


def parse_version_op(s: str) -> VersionOp:
    op = _VERSION_OPS.get(s, None)
    if not op:
        raise Exception(f"unknown version operator: {s}")
    return op


def use_triple(major: int, minor: int | None = None, patch: int | None = None):
    print(major, minor, patch)


@dataclass(frozen=True)
class VersionTriple:
    major: int
    minor: int | None = dataclasses.field(default_factory=lambda: None)
    patch: int | None = dataclasses.field(default_factory=lambda: None)

    def __post_init__(self):
        if self.minor is None:
            if self.patch is not None:
                raise AssertionError(
                    "minor version must be set if patch version is set"
                )

    def bump(self) -> "VersionTriple":
        # Bump the smallest version field that is defined. This allows you to
        # turn an exclusive upper bound into an inclusive upper bound or an
        # inclusive lower bound into an exclusive lower bound.
        major = self.major
        minor = self.minor
        patch = self.patch
        if patch is not None:
            patch += 1
        elif minor is not None:
            minor += 1
        else:
            major += 1
        return VersionTriple(major, minor, patch)

    def bump_compatible(self) -> "VersionTriple":
        # See https://packaging.python.org/en/latest/specifications/version-specifiers/#compatible-release
        major = self.major
        minor = self.minor
        patch = self.patch
        if minor is None:
            raise Exception(
                "~= compatibility operator cannot be used with major-only version"
            )
        if patch is None:
            major = major + 1
        else:
            minor = minor + 1
        return VersionTriple(major, minor, patch)

    def compare(self, other: "VersionTriple") -> int:
        c = self.major - other.major
        if c != 0:
            return c
        c = (self.minor or 0) - (other.minor or 0)
        if c != 0:
            return c
        return (self.patch or 0) - (other.patch or 0)

    def __lt__(self, other: "VersionTriple") -> bool:
        return self.compare(other) < 0

    def __le__(self, other: "VersionTriple") -> bool:
        return self.compare(other) <= 0

    def __gt__(self, other: "VersionTriple") -> bool:
        return self.compare(other) > 0

    def __ge__(self, other: "VersionTriple") -> bool:
        return self.compare(other) >= 0

    @staticmethod
    def parse(s: str) -> "VersionTriple":
        if s.endswith(".*"):
            s = s[-len(".*")]
        v = parse_version(s).release
        print(v)
        return VersionTriple(*v)


@dataclass(frozen=True)
class LowerBound:
    version: VersionTriple | None

    def compare(self, other: Union["LowerBound", "UpperBound"]) -> int:
        match other:
            case LowerBound(version):
                if self.version is None:
                    if version is None:
                        return 0
                    else:
                        return -1
                elif version is None:
                    return 1
                else:
                    return self.version.compare(version)
            case UpperBound(version):
                if self.version is None or version is None:
                    return -1
                return self.version.compare(version)

    def contains(self, version: VersionTriple) -> bool:
        return self.version is None or version >= self.version

    def __lt__(self, other: Union["LowerBound", "UpperBound"]) -> bool:
        return self.compare(other) < 0

    def __le__(self, other: Union["LowerBound", "UpperBound"]) -> bool:
        return self.compare(other) <= 0

    @staticmethod
    def unbounded() -> "LowerBound":
        return LowerBound(None)


@dataclass(frozen=True)
class UpperBound:
    version: VersionTriple | None

    def compare(self, other: Union[LowerBound, "UpperBound"]) -> int:
        match other:
            case LowerBound(version):
                if self.version is None or version is None:
                    return 1
                return self.version.compare(version)
            case UpperBound(version):
                if self.version is None:
                    if version is None:
                        return 0
                    else:
                        return 1
                elif version is None:
                    return -1
                else:
                    return self.version.compare(version)

    def contains(self, version: VersionTriple) -> bool:
        return self.version is None or version < self.version

    def __lt__(self, other: Union[LowerBound, "UpperBound"]) -> bool:
        return self.compare(other) < 0

    def __le__(self, other: Union[LowerBound, "UpperBound"]) -> bool:
        return self.compare(other) <= 0

    @staticmethod
    def unbounded() -> "UpperBound":
        return UpperBound(None)


@dataclass(frozen=True)
class VersionSpan:
    # Lower version bound (inclusive)
    lower: LowerBound
    # Upper version bound (exclusive)
    upper: UpperBound

    def __post_init__(self):
        if self.upper <= self.lower:
            raise Exception(
                f"version span must be non-empty; got: [{self.lower},{self.upper})"
            )

    @staticmethod
    def unbounded() -> "VersionSpan":
        return VersionSpan(LowerBound.unbounded(), UpperBound.unbounded())

    def compare(self, other: "VersionSpan") -> int:
        c = self.lower.compare(other.lower)
        if c != 0:
            return c
        return self.upper.compare(other.upper)

    def contains(self, version: VersionTriple) -> bool:
        return self.lower.contains(version) and self.upper.contains(version)

    def __lt__(self, other: "VersionSpan") -> bool:
        return self.compare(other) < 0


@dataclass(frozen=True)
class FixedVersion:
    version: str


@dataclass(frozen=True)
class DisjointVersionSpan:
    spans: tuple[VersionSpan, ...]

    def contains(self, version: VersionTriple) -> bool:
        for span in self.spans:
            # We could in principle do binary search by keeping the spans
            # sorted, but there should never be enough version specifiers to
            # make this worthwhile. Having so many constraints would almost
            # certainly be a bug.
            if span.contains(version):
                return True
        return False


@dataclass(frozen=True)
class VersionSet:
    version: VersionSpan | FixedVersion | DisjointVersionSpan | None

    @staticmethod
    def empty() -> "VersionSet":
        return VersionSet(None)

    def __and__(self, other: "VersionSet") -> "VersionSet":
        match (self.version, other.version):
            case (None, _):
                return self
            case (_, None):
                return other
            case (FixedVersion(a), FixedVersion(b)):
                if a == b:
                    return self
                else:
                    return VersionSet(None)
            case (FixedVersion(v), VersionSpan(_) as span):
                version = VersionTriple.parse(v)
                if span.contains(version):
                    return self
                else:
                    return VersionSet.empty()
            case (VersionSpan(_) as span, FixedVersion(v)):
                version = VersionTriple.parse(v)
                if span.contains(version):
                    return other
                else:
                    return VersionSet.empty()
            case (FixedVersion(v), DisjointVersionSpan(_) as spans):
                version = VersionTriple.parse(v)
                if spans.contains(version):
                    return self
                else:
                    return VersionSet.empty()
            case (DisjointVersionSpan(_) as spans, FixedVersion(v)):
                version = VersionTriple.parse(v)
                if spans.contains(version):
                    return other
                else:
                    return VersionSet.empty()
            case (VersionSpan(_) as a, VersionSpan(_) as b):
                span = merge_spans(a, b)
                if span is None:
                    return VersionSet.empty()
                else:
                    return VersionSet(span)
            case (VersionSpan(_) as span, DisjointVersionSpan(_) as spans):
                return merge_span_with_disjoint(span, spans)
            case (DisjointVersionSpan(_) as spans, VersionSpan(_) as span):
                return merge_span_with_disjoint(span, spans)
            case (DisjointVersionSpan(_) as a, DisjointVersionSpan(_) as b):
                return merge_disjoint_spans(a, b)
            case _:
                raise AssertionError(f"invalid version set pair: {self}, {other}")

    def is_empty(self) -> bool:
        return self.version is None


def merge_spans(a: VersionSpan, b: VersionSpan) -> VersionSpan | None:
    if a.lower < b.lower:
        lower = b.lower
    else:
        lower = a.lower
    if a.upper < b.upper:
        upper = a.upper
    else:
        upper = b.upper
    if lower < upper:
        return VersionSpan(lower, upper)
    else:
        return None


def merge_span_with_disjoint(
    span: VersionSpan, spans: DisjointVersionSpan
) -> VersionSet:
    for s in spans.spans:
        new_span = merge_spans(span, s)
        if new_span is not None:
            return VersionSet(new_span)
    return VersionSet.empty()


def merge_disjoint_spans(a: DisjointVersionSpan, b: DisjointVersionSpan) -> VersionSet:
    spans: list[VersionSpan] = []
    for left in a.spans:
        # As elsewhere, we could in principle get rid of the n^2 runtime by
        # sorting and skipping as needed, but we don't bother here.
        for right in b.spans:
            span = merge_spans(left, right)
            if span is not None:
                spans.append(span)
    if len(spans) > 0:
        return VersionSet(DisjointVersionSpan(tuple(spans)))
    return VersionSet.empty()


def version_set(op: VersionOp, version: str) -> VersionSet:
    match op:
        case VersionOp.COMPATIBLE:
            lower = VersionTriple.parse(version)
            upper = lower.bump_compatible()
            return VersionSet(VersionSpan(LowerBound(lower), UpperBound(upper)))
        case VersionOp.MATCHING:
            lower = VersionTriple.parse(version)
            upper = lower.bump()
            return VersionSet(VersionSpan(LowerBound(lower), UpperBound(upper)))
        case VersionOp.EXCLUDING:
            v = VersionTriple.parse(version)
            lower_span = VersionSpan(LowerBound(None), UpperBound(v))
            upper_span = VersionSpan(LowerBound(v.bump()), UpperBound(None))
            return VersionSet(DisjointVersionSpan((lower_span, upper_span)))
        case VersionOp.LEQ:
            v = VersionTriple.parse(version)
            upper = v.bump()
            return VersionSet(VersionSpan(LowerBound(None), UpperBound(upper)))
        case VersionOp.GEQ:
            return VersionSet(
                VersionSpan(LowerBound(VersionTriple.parse(version)), UpperBound(None))
            )
        case VersionOp.LT:
            return VersionSet(
                VersionSpan(LowerBound(None), UpperBound(VersionTriple.parse(version)))
            )
        case VersionOp.GT:
            v = VersionTriple.parse(version)
            lower = v.bump()
            return VersionSet(VersionSpan(LowerBound(lower), UpperBound(None)))
        case VersionOp.EQUAL:
            return VersionSet(FixedVersion(version))
        case _:
            raise Exception(f"assertion failure: invalid version operator: {op}")


def is_consistent(specs: SpecifierSet) -> bool:
    # TODO: Implement best-effort consistency check by iterating over specifier
    # set and computing the implied intersection based on `Specifier.version`
    # and `Specifier.operator`.
    joint_spec = VersionSet(VersionSpan.unbounded())
    for spec in specs:
        op = parse_version_op(spec.operator)
        vspec = version_set(op, spec.version)
        joint_spec = joint_spec & vspec
    return not joint_spec.is_empty()


A = TypeVar("A")


class Result(Generic[A]):
    pass


@dataclass(frozen=True)
class Error(Result[A]):
    err: Exception


@dataclass(frozen=True)
class Ok(Result[A]):
    value: A


def use_result(result: Result[A]) -> A:
    match result:
        case Error(err):
            raise err
        case Ok(value):
            return value
        case _:
            raise AssertionError()


if __name__ == "__main__":
    main()
