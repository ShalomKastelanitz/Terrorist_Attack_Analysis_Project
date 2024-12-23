from sqlalchemy import Column, Integer, BigInteger, Float, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class DimDate(Base):
    __tablename__ = 'dim_date'
    date_id = Column(Integer, primary_key=True, autoincrement=True)
    iyear = Column(Integer)
    imonth = Column(Integer)
    iday = Column(Integer)

    events = relationship("FactEvents", back_populates="date")


class DimLocation(Base):
    __tablename__ = 'dim_location'
    location_id = Column(Integer, primary_key=True, autoincrement=True)
    region_txt = Column(String(100))
    country_txt = Column(String(100))
    city = Column(String(255))
    latitude = Column(Float)
    longitude = Column(Float)

    events = relationship("FactEvents", back_populates="location")

class DimAttackType(Base):
    __tablename__ = 'dim_attack_type'
    attacktype_id = Column(Integer, primary_key=True, autoincrement=True)
    attacktype_name = Column(String(255))

    events = relationship("FactEvents", back_populates="attack_type")

class DimTargetType(Base):
    __tablename__ = 'dim_target_type'
    targettype_id = Column(Integer, primary_key=True, autoincrement=True)
    targettype_name = Column(String(255))

    events = relationship("FactEvents", back_populates="target_type")

class DimGroup(Base):
    __tablename__ = 'dim_group'
    group_id = Column(Integer, primary_key=True, autoincrement=True)
    group_name = Column(String(255))

    primary_events = relationship("FactEvents", back_populates="primary_group", foreign_keys="[FactEvents.primary_group_id]")
    secondary_events = relationship("FactEvents", back_populates="secondary_group", foreign_keys="[FactEvents.secondary_group_id]")
    tertiary_events = relationship("FactEvents", back_populates="tertiary_group", foreign_keys="[FactEvents.tertiary_group_id]")

class FactEvents(Base):
    __tablename__ = 'fact_events'
    event_id = Column(BigInteger, primary_key=True)
    date_id = Column(Integer, ForeignKey('dim_date.date_id'))
    location_id = Column(Integer, ForeignKey('dim_location.location_id'))
    attacktype_id = Column(Integer, ForeignKey('dim_attack_type.attacktype_id'))
    targettype_id = Column(Integer, ForeignKey('dim_target_type.targettype_id'))
    primary_group_id = Column(Integer, ForeignKey('dim_group.group_id'))
    secondary_group_id = Column(Integer, ForeignKey('dim_group.group_id'))
    tertiary_group_id = Column(Integer, ForeignKey('dim_group.group_id'))
    nkill = Column(Integer)
    nwound = Column(Integer)

    date = relationship("DimDate", back_populates="events")
    location = relationship("DimLocation", back_populates="events")
    attack_type = relationship("DimAttackType", back_populates="events")
    target_type = relationship("DimTargetType", back_populates="events")
    primary_group = relationship("DimGroup", back_populates="primary_events", foreign_keys=[primary_group_id])
    secondary_group = relationship("DimGroup", back_populates="secondary_events", foreign_keys=[secondary_group_id])
    tertiary_group = relationship("DimGroup", back_populates="tertiary_events", foreign_keys=[tertiary_group_id])
