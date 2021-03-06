import pytest

from django.db.utils import IntegrityError
from django.core.exceptions import ValidationError
from django.utils.translation import ugettext_lazy as _

from tests.share.models import factories


@pytest.mark.django_db
class TestAbstractAgentWorkRelation:

    def test_exists(self):
        x = factories.PreprintFactory(contributors=5)
        assert x.related_agents.count() == 5
        for contributor in x.agent_relations.all():
            assert contributor.creative_work == x
            assert list(contributor.agent.related_works.all()) == [x]

    def test_unique(self):
        pp = factories.PreprintFactory()
        person = factories.AgentFactory(type='share.person')
        factories.AgentWorkRelationFactory(creative_work=pp, agent=person, type='share.creator')

        with pytest.raises(IntegrityError):
            factories.AgentWorkRelationFactory(creative_work=pp, agent=person, type='share.creator')

    def test_many_contribution_types(self):
        pp = factories.PreprintFactory(contributors=0)
        person = factories.AgentFactory(type='share.person')
        funding = factories.AgentWorkRelationFactory(creative_work=pp, agent=person, type='share.funder')

        assert pp.related_agents.count() == 1
        assert pp.agent_relations.count() == 1

        collaboration = factories.AgentWorkRelationFactory(creative_work=pp, agent=person, type='share.creator')

        assert funding != collaboration
        assert pp.related_agents.count() == 2
        assert pp.agent_relations.count() == 2


@pytest.mark.django_db
class TestThroughAgentWorkRelation:

    def test_exists(self):
        pp = factories.PreprintFactory(contributors=0)
        person = factories.AgentFactory(type='share.person')
        consortium = factories.AgentFactory(type='share.consortium')
        consortium_collaboration = factories.AgentWorkRelationFactory(creative_work=pp, agent=consortium, type='share.contributor')
        collaboration = factories.AgentWorkRelationFactory(creative_work=pp, agent=person, type='share.creator')
        factories.ThroughAgentWorkRelationFactory(subject=collaboration, related=consortium_collaboration, change=factories.ChangeFactory())

        assert list(collaboration.contributed_through.all()) == [consortium_collaboration]

    def test_must_share_creative_work(self):
        pp1 = factories.PreprintFactory(contributors=0)
        pp2 = factories.PreprintFactory(contributors=0)
        person = factories.AgentFactory(type='share.person')
        consortium = factories.AgentFactory(type='share.consortium')

        consortium_collaboration = factories.AgentWorkRelationFactory(creative_work=pp1, agent=consortium, type='share.creator')
        collaboration = factories.AgentWorkRelationFactory(creative_work=pp2, agent=person, type='share.creator')

        with pytest.raises(ValidationError) as e:
            factories.ThroughAgentWorkRelationFactory(subject=collaboration, related=consortium_collaboration)
        assert e.value.args == (_('ThroughContributors must contribute to the same AbstractCreativeWork'), None, None)

    def test_cannot_be_self(self):
        pp = factories.PreprintFactory(contributors=0)
        person = factories.AgentFactory(type='share.person')
        collaboration = factories.AgentWorkRelationFactory(creative_work=pp, agent=person, type='share.creator')

        with pytest.raises(ValidationError) as e:
            factories.ThroughAgentWorkRelationFactory(subject=collaboration, related=collaboration)
        assert e.value.args == (_('A contributor may not contribute through itself'), None, None)
