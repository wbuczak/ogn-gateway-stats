package org.ogn.gateway.plugin.stats;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ogn.commons.beacon.AircraftBeacon;
import org.ogn.commons.beacon.ReceiverBeacon;

/**
 * Copyright (c) 2015-2018 OGN, All Rights Reserved.
 */

@RunWith(EasyMockRunner.class)
public class StatsTest {

	@Mock
	AircraftBeacon	b1	= null;

	@Mock
	ReceiverBeacon	r1	= null;

	@Test
	public void testReceptionSignalValidationAlgo() throws Exception {

		final Stats stats = new Stats();

		expect(b1.getSignalStrength()).andReturn(40.2f).anyTimes();
		replay(b1);

		assertFalse(stats.isValidSignalStrength4Distance(151, b1));
		assertTrue(stats.isValidSignalStrength4Distance(5, b1));

		reset(b1);
		expect(b1.getSignalStrength()).andReturn(18.2f).anyTimes();
		replay(b1);

		assertTrue(stats.isValidSignalStrength4Distance(120, b1));
		assertFalse(stats.isValidSignalStrength4Distance(320, b1));
		assertFalse(stats.isValidSignalStrength4Distance(800, b1));
	}
}
