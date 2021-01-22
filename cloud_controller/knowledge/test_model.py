import unittest

from cloud_controller.analysis.trivial_solver.trivial_solver import get_first_managed_component
from cloud_controller.knowledge.model import Component, Application, ComponentType, CloudState, ManagedCompin


class ComponentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = Application("sample_app")

        # client --> comp1 --> comp1-1 --> comp1-1-1
        #                  \
        #                   -> comp1-2
        self.client = Component(self.app, "client", "client", ComponentType.UNMANAGED)
        self.comp1 = Component(self.app, "comp1", "comp1", ComponentType.MANAGED)
        self.comp1_1 = Component(self.app, "comp1-1", "comp1-1", ComponentType.MANAGED)
        self.comp1_2 = Component(self.app, "comp1-2", "comp1-2", ComponentType.MANAGED)
        self.comp1_1_1 = Component(self.app, "comp1-1-1", "comp1-1-1", ComponentType.MANAGED)
        self._managed_components = [self.comp1, self.comp1_1, self.comp1_2, self.comp1_1_1]
        self.client.add_dependency(self.comp1)
        self.comp1.add_dependency(self.comp1_1)
        self.comp1.add_dependency(self.comp1_2)
        self.comp1_1.add_dependency(self.comp1_1_1)

        # Dependencies for client.
        self._dependencies_in_levels = [[self.comp1], [self.comp1_1, self.comp1_2], [self.comp1_1_1]]
        self._dependency_chains = [[self.comp1, self.comp1_1, self.comp1_1_1], [self.comp1, self.comp1_2]]

        self.app.add_components([self.client] + self._managed_components)

    def test_list_managed_components(self):
        managed_comps = list(self.app.list_managed_components())
        self.assertEqual(len(managed_comps), len(self._managed_components))

        found_ids = (component.id for component in managed_comps)
        for expected_id in (component.id for component in self._managed_components):
            self.assertIn(expected_id, found_ids)

    def test_get_all_dependencies_in_levels(self):
        deps_in_levels = self.client.get_all_dependencies_in_levels()
        expected_deps_in_levels = self._dependencies_in_levels
        for level, expected_level in zip(deps_in_levels, expected_deps_in_levels):
            for component, expected_component in zip(level, expected_level):
                self.assertEqual(component, expected_component)

    def test_get_dependency_chains(self):
        dep_chains = self.client.get_dependency_chains()
        expected_dep_chains = self._dependency_chains
        for chain, expected_chain in zip(dep_chains, expected_dep_chains):
            for component, expected_component in zip(chain, expected_chain):
                self.assertEqual(component, expected_component)


class ApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = Application("sample_app")

        # client --> comp1 --> comp2 --> comp3
        self.client = Component(self.app, "client", "client", ComponentType.UNMANAGED)
        self.comp1 = Component(self.app, "comp1", "comp1", ComponentType.MANAGED)
        self.comp2 = Component(self.app, "comp2", "comp2", ComponentType.MANAGED)
        self.comp3 = Component(self.app, "comp3", "comp3", ComponentType.MANAGED)
        self.client.add_dependency(self.comp1)
        self.comp1.add_dependency(self.comp2)
        self.comp2.add_dependency(self.comp3)

        self.managed_components = [self.comp1, self.comp2, self.comp3]
        self.unmanaged_components = [self.client]

        self.app.add_components([self.client, self.comp1, self.comp2, self.comp3])

    def test_get_first_managed_component(self):
        first_managed_component = get_first_managed_component(self.app)
        self.assertEqual(first_managed_component, self.comp1)

    def test_list_managed_components(self):
        for managed_component in self.app.list_managed_components():
            self.assertTrue(managed_component in self.managed_components)

    def test_list_unmanaged_components(self):
        for unmanaged_component in self.app.list_unmanaged_components():
            self.assertTrue(unmanaged_component in self.unmanaged_components)


class CloudStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self._create_first_application()
        self._create_second_application()
        self.all_compins = [self.compin1, self.compin2, self.second_compin]
        self._create_cloud_state(self.all_compins)

    def _create_first_application(self):
        self.app = Application("sample_application")

        # component1 --> component2
        self.component1 = Component(self.app, "component1", "component1", ComponentType.MANAGED)
        self.component2 = Component(self.app, "component2", "component2", ComponentType.MANAGED)
        self.component1.add_dependency(self.component2)
        self.app.add_components([self.component1, self.component2])

        # Note there is no dependency between compins on purpose.
        self.compin1 = ManagedCompin(self.component1, id_="compin1", node="node", chain_id="1")
        self.compin2 = ManagedCompin(self.component2, id_="compin2", node="node", chain_id="1")

    def _create_second_application(self):
        self.second_app = Application("second_application")
        self.second_component = Component(self.second_app, "second_component", "second_component",
                                          ComponentType.MANAGED)
        self.second_app.add_component(self.second_component)
        self.second_compin = ManagedCompin(self.second_component, id_="second_compin", node="node", chain_id="2")

    def _create_cloud_state(self, all_compins):
        self.cloud_state = CloudState()
        self.cloud_state.add_application(self.app)
        self.cloud_state.add_application(self.second_app)
        self.cloud_state.add_instances(all_compins)

    def test_get_compin(self):
        compin = self.cloud_state.get_compin(self.app.name, self.component1.name, self.compin1.id)
        self.assertEqual(compin, self.compin1)

        compin2 = self.cloud_state.get_compin(self.app.name, self.component2.name, self.compin2.id)
        self.assertEqual(compin2, self.compin2)

        no_compin = self.cloud_state.get_compin("None-existing-application", "non-existing-component", "compin-id")
        self.assertIsNone(no_compin)

    def test_set_dependency(self):
        self.cloud_state.set_dependency(self.app.name, self.component1.name, id_=self.compin1.id,
                                        dependency=self.component2.name, dependency_id=self.compin2.id)
        self.assertIsNotNone(self.compin1.get_dependency(self.component2.name))

    def test_list_all_compins(self):
        for managed_compin in self.cloud_state.list_all_managed_compins():
            self.assertTrue(managed_compin in self.all_compins)

        self.assertEqual(len(list(self.cloud_state.list_all_unmanaged_compins())), 0)
