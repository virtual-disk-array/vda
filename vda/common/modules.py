from sqlalchemy import (
    Column,
    BigInteger,
    Integer,
    String,
    Text,
    ForeignKey,
    UniqueConstraint,
    Boolean,
)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from vda.common.constant import Constant


Base = declarative_base()


class DiskArray(Base):

    __tablename__ = "disk_array"

    da_id = Column(
        String(Constant.RES_ID_LENGTH),
        primary_key=True,
    )

    da_name = Column(
        String(Constant.RES_NAME_LENGTH),
        nullable=False,
        unique=True,
    )

    cntlr_cnt = Column(Integer, nullable=False)

    da_size = Column(BigInteger, nullable=False)

    da_conf = Column(Text, nullable=False)

    da_details = Column(Text, nullable=False)

    hash_code = Column(Integer, nullable=False)

    error = Column(Boolean, nullable=False)

    cntlrs = relationship("Controller", back_populates="da")

    grps = relationship("Group", back_populates="da")

    exps = relationship("Exporter", back_populates="da")


class Group(Base):

    __tablename__ = "group"

    grp_id = Column(
        String(Constant.RES_ID_LENGTH),
        primary_key=True,
    )

    grp_idx = Column(Integer, nullable=False)

    grp_size = Column(BigInteger, nullable=False)

    da_id = Column(
        String(Constant.RES_ID_LENGTH),
        ForeignKey("disk_array.da_id"),
        nullable=False,
    )

    da = relationship("DiskArray", back_populates="grps")

    error = Column(Boolean, nullable=False)

    error_msg = Column(Text)

    vds = relationship("VirtualDisk", back_populates="grp")


class DiskNode(Base):

    __tablename__ = "disk_node"

    dn_id = Column(
        String(Constant.RES_ID_LENGTH),
        primary_key=True,
    )

    dn_name = Column(
        String(Constant.RES_NAME_LENGTH),
        nullable=False,
        unique=True,
    )

    dn_listener_conf = Column(
        Text,
        nullable=False,
    )

    online = Column(Boolean, nullable=False)

    location = Column(
        String(Constant.RES_NAME_LENGTH),
        nullable=True,
    )

    hash_code = Column(Integer, nullable=False)

    version = Column(BigInteger, nullable=False)

    pds = relationship("PhysicalDisk", back_populates="dn")

    error = Column(Boolean, nullable=False)

    error_msg = Column(Text)


class PhysicalDisk(Base):

    __tablename__ = "physical_disk"

    __table_args__ = (
        UniqueConstraint("pd_name", "dn_id", name="dn_pd"),
    )

    pd_id = Column(
        String(Constant.RES_ID_LENGTH),
        primary_key=True,
    )

    pd_name = Column(
        String(Constant.RES_NAME_LENGTH),
        nullable=False,
    )

    total_clusters = Column(BigInteger, nullable=False)

    free_clusters = Column(BigInteger, nullable=False)

    pd_conf = Column(Text, nullable=False)

    online = Column(Boolean, nullable=False)

    dn_id = Column(
        String(Constant.RES_ID_LENGTH),
        ForeignKey("disk_node.dn_id"),
        nullable=False,
    )

    dn = relationship("DiskNode", back_populates="pds")

    vds = relationship("VirtualDisk", back_populates="pd")

    error = Column(Boolean, nullable=False)

    error_msg = Column(Text)


class VirtualDisk(Base):

    __tablename__ = "virtual_disk"

    vd_id = Column(
        String(Constant.RES_ID_LENGTH),
        primary_key=True,
    )

    vd_idx = Column(Integer, nullable=False)

    vd_clusters = Column(BigInteger, nullable=False)

    grp_id = Column(
        String(Constant.RES_ID_LENGTH),
        ForeignKey("group.grp_id"),
        nullable=False,
    )

    grp = relationship("Group", back_populates="vds")

    pd_id = Column(
        String(Constant.RES_ID_LENGTH),
        ForeignKey("physical_disk.pd_id"),
        nullable=False,
    )

    pd = relationship("PhysicalDisk", back_populates="vds")

    dn_error = Column(Boolean, nullable=False)

    dn_error_msg = Column(Text)

    cn_error = Column(Boolean, nullable=False)

    cn_error_msg = Column(Text)


class ControllerNode(Base):

    __tablename__ = "controller_node"

    cn_id = Column(
        String(Constant.RES_ID_LENGTH),
        primary_key=True,
    )

    cn_name = Column(
        String(Constant.RES_NAME_LENGTH),
        nullable=False,
        unique=True,
    )

    cn_listener_conf = Column(
        Text,
        nullable=False,
    )

    online = Column(Boolean, nullable=False)

    location = Column(
        String(Constant.RES_NAME_LENGTH),
        nullable=True,
    )

    hash_code = Column(Integer, nullable=False)

    version = Column(BigInteger, nullable=False)

    cntlrs = relationship("Controller", back_populates="cn")

    error = Column(Boolean, nullable=False)

    error_msg = Column(Text)


class Controller(Base):

    __tablename__ = "controller"

    cntlr_id = Column(
        String(Constant.RES_ID_LENGTH),
        primary_key=True,
    )

    cntlr_idx = Column(Integer, nullable=False)

    primary = Column(Boolean, nullable=False)

    da_id = Column(
        String(Constant.RES_ID_LENGTH),
        ForeignKey("disk_array.da_id"),
        nullable=False,
    )

    da = relationship("DiskArray", back_populates="cntlrs")

    cn_id = Column(
        String(Constant.RES_ID_LENGTH),
        ForeignKey("controller_node.cn_id"),
        nullable=False,
    )

    cn = relationship("ControllerNode", back_populates="cntlrs")

    ess = relationship("ExpState", back_populates="cntlr")

    error = Column(Boolean, nullable=False)

    error_msg = Column(Text)


class Exporter(Base):

    __tablename__ = "exporter"

    __table_args__ = (
        UniqueConstraint("exp_name", "da_id", name="da_exp"),
    )

    exp_id = Column(
        String(Constant.RES_ID_LENGTH),
        primary_key=True,
    )

    exp_name = Column(
        String(Constant.RES_NAME_LENGTH),
        nullable=False,
    )

    da_id = Column(
        String(Constant.RES_ID_LENGTH),
        ForeignKey("disk_array.da_id"),
        nullable=False,
    )

    da = relationship("DiskArray", back_populates="exps")

    initiator_nqn = Column(
        String(Constant.RES_NAME_LENGTH),
    )

    snap_name = Column(
        String(Constant.RES_NAME_LENGTH),
        nullable=True,
    )

    ess = relationship("ExpState", back_populates="exp")


class ExpState(Base):

    __tablename__ = "exp_state"

    es_id = Column(
        String(Constant.RES_ID_LENGTH),
        primary_key=True,
    )

    exp_id = Column(
        String(Constant.RES_ID_LENGTH),
        ForeignKey("exporter.exp_id"),
        nullable=False,
    )

    exp = relationship("Exporter", back_populates="ess")

    cntlr_id = Column(
        String(Constant.RES_ID_LENGTH),
        ForeignKey("controller.cntlr_id"),
        nullable=False,
    )

    cntlr = relationship("Controller", back_populates="ess")

    error = Column(Boolean, nullable=False)

    error_msg = Column(Text)
