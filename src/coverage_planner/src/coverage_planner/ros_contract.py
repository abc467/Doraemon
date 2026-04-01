#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import inspect
import os
import sys
from typing import Dict, Iterable, Mapping, Optional, Type


def describe_ros_type(cls: Type[object]) -> Dict[str, str]:
    module_name = str(getattr(cls, "__module__", "") or "")
    module = sys.modules.get(module_name)
    source_path = ""
    try:
        if module is not None:
            source_path = inspect.getsourcefile(module) or inspect.getfile(module) or ""
    except Exception:
        source_path = ""
    if not source_path:
        try:
            source_path = inspect.getsourcefile(cls) or inspect.getfile(cls) or ""
        except Exception:
            source_path = ""
    return {
        "name": str(getattr(cls, "__name__", "") or ""),
        "type": str(getattr(cls, "_type", "") or ""),
        "md5": str(getattr(cls, "_md5sum", "") or ""),
        "module": module_name,
        "source_path": os.path.abspath(source_path) if source_path else "",
    }


def validate_ros_contract(
    label: str,
    cls: Type[object],
    *,
    required_fields: Optional[Iterable[str]] = None,
    required_constants: Optional[Iterable[str]] = None,
) -> Dict[str, str]:
    info = describe_ros_type(cls)
    missing_fields = []
    missing_constants = []

    required_fields = list(required_fields or [])
    required_constants = list(required_constants or [])

    slots = {str(item) for item in (getattr(cls, "__slots__", []) or [])}
    for field_name in required_fields:
        key = str(field_name or "").strip()
        if key and key not in slots:
            missing_fields.append(key)

    for const_name in required_constants:
        key = str(const_name or "").strip()
        if key and not hasattr(cls, key):
            missing_constants.append(key)

    if missing_fields or missing_constants:
        detail_parts = []
        if missing_fields:
            detail_parts.append("missing_fields=%s" % ",".join(missing_fields))
        if missing_constants:
            detail_parts.append("missing_constants=%s" % ",".join(missing_constants))
        detail = " ".join(detail_parts).strip()
        raise RuntimeError(
            (
                "%s contract mismatch: %s source=%s md5=%s"
                % (
                    str(label or "").strip() or "ros_contract",
                    detail or "definition mismatch",
                    info.get("source_path", ""),
                    info.get("md5", ""),
                )
            ).strip()
        )
    return info


def build_contract_report(
    *,
    service_name: str,
    contract_name: str,
    service_cls: Type[object],
    request_cls: Type[object],
    response_cls: Type[object],
    dependencies: Optional[Mapping[str, Type[object]]] = None,
    features: Optional[Iterable[str]] = None,
) -> Dict[str, object]:
    dependency_info = {}
    for key, value in dict(dependencies or {}).items():
        dependency_info[str(key)] = describe_ros_type(value)
    return {
        "contract_name": str(contract_name or "").strip(),
        "service_name": str(service_name or "").strip(),
        "service": describe_ros_type(service_cls),
        "request": describe_ros_type(request_cls),
        "response": describe_ros_type(response_cls),
        "dependencies": dependency_info,
        "features": [str(item) for item in (features or []) if str(item or "").strip()],
        "python_executable": str(sys.executable or ""),
    }
