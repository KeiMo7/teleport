/*
 * Teleport
 * Copyright (C) 2025  Gravitational, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package recordingindex

import (
	"github.com/gravitational/trace"

	apidefaults "github.com/gravitational/teleport/api/defaults"
	"github.com/gravitational/teleport/api/types"
	"github.com/gravitational/teleport/api/types/wrappers"
	"github.com/gravitational/teleport/lib/services"
)

// fakeChecker embeds the AccessChecker interface so only the methods exercised
// by the adapter tests need concrete implementations.
type fakeChecker struct {
	services.AccessChecker

	roles       []types.Role
	traits      wrappers.Traits
	guessErr    error
	simpleErr   error
	extendedErr error

	ruleChecks int
}

func (c *fakeChecker) Traits() wrappers.Traits { return c.traits }

func (c *fakeChecker) Roles() []types.Role { return c.roles }

func (c *fakeChecker) RoleNames() []string {
	names := make([]string, 0, len(c.roles))
	for _, role := range c.roles {
		names = append(names, role.GetName())
	}
	return names
}

func (c *fakeChecker) GuessIfAccessIsPossible(_ services.RuleContext, _, _, _ string) error {
	return c.guessErr
}

// CheckAccessToRule returns the simple error for a bare context and the
// configured extended error once the context carries a session event.
func (c *fakeChecker) CheckAccessToRule(ruleCtx services.RuleContext, _, rule, verb string) error {
	c.ruleChecks++
	ctx, ok := ruleCtx.(*services.Context)
	if ok && ctx != nil && ctx.Session != nil {
		return c.extendedErr
	}
	_ = apidefaults.Namespace
	_ = rule
	_ = verb
	return c.simpleErr
}

// fakeUserState implements services.UserState.
type fakeUserState struct {
	name string
}

func (u fakeUserState) GetName() string                { return u.name }
func (u fakeUserState) GetRoles() []string             { return nil }
func (u fakeUserState) GetTraits() map[string][]string { return nil }
func (u fakeUserState) GetUserType() types.UserType    { return types.UserTypeLocal }
func (u fakeUserState) GetLabel(string) (string, bool) { return "", false }
func (u fakeUserState) IsBot() bool                    { return false }
func (u fakeUserState) GetGithubIdentities() []types.ExternalIdentity {
	return nil
}
func (u fakeUserState) SetGithubIdentities([]types.ExternalIdentity) {}

// denyError is a stable AccessDenied used by the fake checker.
func denyError(message string) error {
	return trace.AccessDenied("%s", message)
}

// ensure fakeChecker satisfies the interface at compile time.
var _ services.AccessChecker = (*fakeChecker)(nil)
